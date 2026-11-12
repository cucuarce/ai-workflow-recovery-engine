# Etapa 3 — Integración con n8n (runbook)

n8n **orquesta**: recibe pedidos por webhook y dispara periódicamente el worker. Toda la lógica
de recuperación (reintentos, lease, fencing, reconciliación, dead-letter) sigue en Python, dentro
del motor. n8n solo llama a la API HTTP del motor y **no tiene acceso a sus bases SQLite**.

---

## 1. Arquitectura

```
                         host: solo 127.0.0.1
 cliente ──POST──▶ 127.0.0.1:5678 ┌──────────── red interna de compose ─────────────┐
 operador ───────▶ 127.0.0.1:8000 │                                                 │
                                  │  n8n ──HTTP + Bearer──▶ engine (FastAPI, 1 proc) │
                                  │   │                        │                     │
                                  │  volumen n8n-data         volumen engine-data    │
                                  │  (SQLite de n8n,          (engine.db,            │
                                  │   credenciales cifradas)   external_simulator.db)│
                                  └─────────────────────────────────────────────────┘
```

- `compose.yaml` define **exactamente dos servicios**: `engine` y `n8n`. No hay Postgres, Redis
  ni colas. n8n usa su SQLite por defecto en su propio volumen.
- Los puertos se publican **solo en `127.0.0.1`**. n8n llega al motor como `http://engine:8000`.
- Las bases del motor viven en un **volumen con nombre**, no en un bind mount. SQLite con WAL
  sobre carpetas compartidas de Windows u OneDrive no es confiable.
- El motor corre como usuario sin privilegios, con sistema de archivos de solo lectura salvo
  `/app/data` y `/tmp`, sin capabilities y con `no-new-privileges`.
- n8n corre con `N8N_BLOCK_ENV_ACCESS_IN_NODE=true` y excluye los nodos Execute Command,
  Local File Trigger y Read/Write Files. Diagnósticos, plantillas y avisos de versión están
  apagados, así que no llama a servicios de terceros.

### Credenciales separadas

| Secreto (`.env`) | Lo usa | Da acceso a |
|---|---|---|
| `ENGINE_API_TOKEN` | Credencial n8n **Recovery Engine API** (Workflow 1) | `/operations*` |
| `ADMIN_API_TOKEN` | Credencial n8n **Recovery Engine Admin** (Workflow 2) y el operador | `/worker/run`; además `/simulator/*` y `/simulator/fault-plans/*` (solo en `demo`/`local`) |
| `WEBHOOK_SECRET` | Credencial n8n **Recovery Webhook Inbound** y quien llama al webhook | El webhook del Workflow 1 (header `X-Recovery-Webhook-Token`) |
| `N8N_ENCRYPTION_KEY` | n8n | Cifra las credenciales guardadas en su base |

- Un token no sirve en la superficie del otro: el token admin en `/operations` da `403`, y el
  token de API en `/worker/run` también da `403`.
- El Workflow 1 **no** tiene acceso al token administrativo.
- Los tokens nunca aparecen en los JSON de los workflows. Los nodos referencian las
  credenciales solo por `id` y `name`.
- **Alcance:** el webhook usa Header Auth con un secreto compartido, y el motor, tokens Bearer
  compartidos. Es adecuado para `127.0.0.1` y la red interna de compose. Para exponer el
  webhook o la API fuera de la máquina se requieren, como mínimo, TLS y controles adicionales
  (por ejemplo, un proxy con límite de tasa, rotación de secretos y restricción de orígenes).
  Este proyecto no los incluye.

**Formato de los errores de autenticación.** Son las respuestas estándar de FastAPI
(`HTTPException`), con el cuerpo anidado en `detail`, distinto del formato plano
`{"error", "detail"}` del resto de los errores del motor:

```json
401  {"detail": {"error": "unauthorized", "detail": "Falta el token Bearer"}}   + WWW-Authenticate: Bearer
403  {"detail": {"error": "forbidden", "detail": "Token inválido para este recurso"}}
```

Se conserva así a propósito; el código de estado distingue los casos.

---

## 2. Configuración segura por defecto

`Settings` valida sus combinaciones al construirse. Si una configuración es inválida, la API y
la CLI **no arrancan**.

| `APP_ENV` | Token de API | Admin | Inyección de fallos | `scenario` en `execute` | Rutas `/simulator/*` |
|---|---|---|---|---|---|
| `production` (**default**) | obligatorio | opcional, con `ADMIN_API_TOKEN` | **prohibida** | rechazado (`403`) | **nunca se montan** (`404`), aun con token admin |
| `demo` (Docker + n8n) | obligatorio | con `ADMIN_API_TOKEN` | permitida, solo por la interfaz admin | rechazado (`403`) | con token admin |
| `local` (desarrollo y tests) | opcional | con `ADMIN_API_TOKEN` | permitida | aceptado | con token admin si está configurado |

Reglas que siempre se aplican:
- tokens de al menos 32 caracteres;
- `ENGINE_API_TOKEN` distinto de `ADMIN_API_TOKEN`;
- `ENABLE_ADMIN_ENDPOINTS=true` exige `ADMIN_API_TOKEN` en cualquier entorno;
- fuera de `local`, la observación del simulador exige `ADMIN_API_TOKEN`.

Además, las variables booleanas de entorno exigen valores válidos. Un valor como `"tal-vez"` en
`FAULT_INJECTION_ENABLED` es un error de arranque, no se toma como `false`.

En `demo`, quien llama al webhook **no puede elegir el fallo**. El operador lo configura antes,
por la interfaz administrativa local, indicando la `Idempotency-Key` del pedido:

```
PUT /simulator/fault-plans/by-idempotency-key/{idempotency_key}   (Bearer ADMIN_API_TOKEN)
```

Después se ejecuta el pedido normal. El plan se guarda del lado del simulador y se aplica a los
envíos de esa operación lógica. Si la operación también tiene un plan propio
(`PUT /simulator/fault-plans/{operation_id}`), ese plan propio reemplaza al plan por clave.

---

## 3. Workflow 1 — Intake (`n8n/workflows/01-intake.json`)

```
Webhook entrante ─▶ ¿Pedido válido? ─no─▶ Responder pedido inválido (400)
                          │sí
                          ▼
                 Registrar operación ─▶ ¿Registrada? ─no─▶ Responder rechazo del motor (409/422 o 502)
                                             │sí
                                             ▼
                              ¿Primer intento pendiente? ─no─┐
                                             │sí              │
                                             ▼                ▼
                                 Ejecutar primer intento ─▶ Consultar estado ─▶ Responder estado
```

| Nodo | Tipo | Configuración |
|---|---|---|
| Webhook entrante | Webhook 2.2 | `POST /webhook/operations`, Header Auth (credencial *Recovery Webhook Inbound*), responde por nodo. |
| ¿Pedido válido? | If 2.2 | Exige el header `Idempotency-Key` y `operation_type`, y que el cuerpo no tenga otros campos que `operation_type` y `payload`. **n8n nunca genera la clave**: si lo hiciera, un reintento del cliente crearía otra operación. |
| Registrar operación | HTTP Request 4.2 | `POST http://engine:8000/operations` con *Recovery Engine API*. Reenvía `Idempotency-Key` tal como llega y **solo** `operation_type` y `payload` (cualquier otro campo ya fue rechazado en el nodo anterior). Hasta 3 reintentos ante errores de red, que es seguro porque el endpoint es idempotente. Timeout de 10 s. |
| ¿Registrada? | If 2.2 | `201` (nueva) o `200` (replay) continúan. Si no, responde `409` o `422` del motor tal cual, y `502` para cualquier otro caso. |
| ¿Primer intento pendiente? | If 2.2 | Solo si `status == pending` llama a `execute`. Un replay de una operación ya avanzada no se vuelve a ejecutar. |
| Ejecutar primer intento | HTTP Request 4.2 | `POST /operations/{id}/execute` **sin body**, sin reintentos del nodo y con timeout de 30 s. Un `409` (otro ejecutor la tomó primero) no es un error: se consulta el estado. |
| Consultar estado | HTTP Request 4.2 | `GET /operations/{id}`, que es idempotente (hasta 3 reintentos). |
| Responder estado | Respond to Webhook 1.5 | `200` si `succeeded`; `422` si `failed_permanent`; **`409` si `dead_letter`** (la recuperación automática terminó y requiere intervención; el motivo va en `reason`); `202` solo para los estados que siguen en recuperación automática (`pending`, `in_progress`, `failed_retryable`, `outcome_unknown`); `502` si el motor no responde. |
| Responder pedido inválido | Respond to Webhook 1.5 | `400` con `invalid_request` (falta la clave u `operation_type`) o `unexpected_fields` (campos no permitidos, listados en `fields`). Nada llega al motor. |

Cuerpo de la respuesta:

```json
{"operation_id": "op_…", "idempotency_key": "…", "status": "outcome_unknown",
 "attempt_count": 1, "external_effect_id": null, "last_error": "timeout: …", "replayed": false}
```

Con `dead_letter` (`409`) se agrega `reason`, el motivo registrado por el motor:

```json
{"operation_id": "op_…", "status": "dead_letter", "attempt_count": 5,
 "reason": "service_unavailable: … (max_attempts_exhausted (5))", "replayed": true, …}
```

Un `409` también puede venir de *Responder rechazo del motor* (clave reutilizada con otro
payload). Se distinguen por el cuerpo: ese caso trae `error` y no `status`.

Un campo como `scenario` se rechaza **antes** de llamar al motor:

```json
400  {"error": "unexpected_fields", "fields": ["scenario"],
      "detail": "Solo se aceptan operation_type y payload. Los fallos del proveedor no pueden elegirse desde el webhook."}
```

El motor aplica la misma regla por su cuenta: `POST /operations` rechaza campos extra (`422`) y
`execute` rechaza `scenario` fuera de `local` (`403`).

**Datos de ejecución:** el Workflow 1 no guarda ejecuciones, ni exitosas ni fallidas, porque n8n
incluye los headers del webhook (también el secreto entrante) en los datos de ejecución. La
auditoría completa vive en el motor: operaciones, intentos y transiciones.

## 4. Workflow 2 — Recovery (`n8n/workflows/02-recovery.json`)

```
Cada 30 segundos ─▶ Ejecutar worker ─▶ ¿Requiere atención? ─sí─▶ Marcar ejecución con error
                                                          └no─▶ Sin novedades
```

| Nodo | Tipo | Configuración |
|---|---|---|
| Cada 30 segundos | Schedule Trigger 1.2 | 30 s en la demo. Fuera de la demo conviene alrededor de 1 minuto. |
| Ejecutar worker | HTTP Request 4.2 | `POST http://engine:8000/worker/run?max_operations=25&worker_id=n8n-recovery` con *Recovery Engine Admin*. Timeout de 60 s, **sin reintentos del nodo**. Un fallo del motor queda como ejecución fallida en n8n. |
| ¿Requiere atención? | If 2.2 | Detecta ítems con `final_status = dead_letter` o `action` igual a `lease_lost` o `error_left_for_lease_expiry`. |
| Marcar ejecución con error | Stop and Error | Deja la ejecución en rojo en el historial de n8n con los IDs afectados. No agrega notificaciones ni servicios externos. |
| Sin novedades | No Operation | — |

Si dos corridas se solapan no pasa nada: el worker es acotado, reclama por compare-and-set con
lease y fencing, y procesa cada operación como máximo una vez por corrida. Las ejecuciones
exitosas no se guardan (una cada 30 s sería ruido); las fallidas sí.

### Operaciones `pending` abandonadas

Si n8n registra la operación y falla antes del primer `execute` (caída, timeout o reinicio), la
operación queda en `pending` con `next_attempt_at = NULL`. El worker **ya** la considera
reclamable (`find_claimable_ids`), así que la siguiente corrida del Workflow 2 la toma y la
ejecuta. No hizo falta cambiar código; los tests de `tests/integration/test_n8n_flows.py` lo
comprueban, junto con la carrera entre el intake y el worker y el webhook duplicado.

---

## 5. Puesta en marcha

Requisitos: Docker Desktop (o Docker Engine con Compose v2) y `uv` solo para el smoke test.

```bash
cp .env.example .env
# Reemplazar CADA "replace-me" por un valor aleatorio distinto (>= 32 caracteres):
python -c "import secrets; print(secrets.token_urlsafe(32))"
docker compose up -d --build --wait
```

### Opción A — Provisión automática (la que usa el smoke test)

```bash
uv run python scripts/smoke_compose.py
```

El script hace todo lo siguiente:
- crea las tres credenciales en n8n desde `.env`; el JSON viaja por stdin y solo existe en
  `/tmp` del contenedor durante la importación;
- importa y publica los dos workflows y reinicia n8n;
- corre todas las verificaciones de la sección 7.

Se puede volver a ejecutar sin problemas: cada corrida usa claves nuevas.

### Opción B — Importación manual en la interfaz

1. Abrir `http://127.0.0.1:5678` y crear la cuenta owner. Es local: queda en el volumen `n8n-data`.
2. **Credentials → Create**. Los nombres deben coincidir exactamente con estos:
   - *Recovery Webhook Inbound*: tipo **Header Auth**, name `X-Recovery-Webhook-Token`, value `WEBHOOK_SECRET`.
   - *Recovery Engine API*: tipo **Bearer Auth**, token `ENGINE_API_TOKEN`.
   - *Recovery Engine Admin*: tipo **Bearer Auth**, token `ADMIN_API_TOKEN`.
3. **Workflows → Import from File**: `n8n/workflows/01-intake.json` y `02-recovery.json`.
   También se puede por CLI:
   `docker compose exec n8n n8n import:workflow --separate --input=/workflows`.
4. En cada nodo con credencial, elegir la credencial correspondiente si n8n la marca como faltante.
5. **Publish/Activate** ambos workflows.

---

## 6. Demo: timeout después del efecto → reconciliación → un solo efecto

Variables de shell para la demo (no se guardan en ningún archivo):

```bash
set -a; . ./.env; set +a          # PowerShell: ver nota al final de esta sección
KEY=demo-$(date +%s)
```

1. **El operador configura el fallo**, antes del pedido y con el token administrativo:
   ```bash
   curl -s -X PUT "http://127.0.0.1:8000/simulator/fault-plans/by-idempotency-key/$KEY" \
     -H "Authorization: Bearer $ADMIN_API_TOKEN" -H 'Content-Type: application/json' \
     -d '{"faults":[{"scenario":"timeout_after_processing"}]}'
   ```
2. **Pedido normal por el webhook**, sin ningún campo de fallos (un `scenario` daría `400`):
   ```bash
   curl -s -i -X POST http://127.0.0.1:5678/webhook/operations \
     -H "X-Recovery-Webhook-Token: $WEBHOOK_SECRET" -H "Idempotency-Key: $KEY" \
     -H 'Content-Type: application/json' \
     -d '{"operation_type":"issue_invoice","payload":{"customer_id":"c-001","amount_cents":15000}}'
   ```
   Resultado esperado: **`202`** con `"status": "outcome_unknown"`. Anotar `OP=<operation_id>`.
3. **El proveedor sí aplicó el efecto**, aunque el motor no recibió la respuesta:
   ```bash
   curl -s "http://127.0.0.1:8000/simulator/requests?operation_ref=$OP" \
     -H "Authorization: Bearer $ADMIN_API_TOKEN"
   # 1 pedido: effect_applied=true, delivered_to_client=false
   ```
4. **Reconciliación.** Esperar a lo sumo 30 s (Schedule) o ejecutar el Workflow 2 a mano en la
   interfaz:
   ```bash
   curl -s "http://127.0.0.1:8000/operations/$OP" -H "Authorization: Bearer $ENGINE_API_TOKEN"
   # status=succeeded; attempts: submit/timeout (api), reconcile/effect_found (n8n-recovery)
   ```
5. **Evidencia final** (con el token admin):
   - `/simulator/requests?operation_ref=$OP` → **1** solicitud, la original; no hubo reenvío;
   - `/simulator/effects?operation_ref=$OP` → **1** efecto, cuyo `effect_id` coincide con
     `external_effect_id` de la operación;
   - `/simulator/lookups?idempotency_key=$OP` → una consulta con `found=true`;
   - `/simulator/duplicates` → `[]`.
6. **Webhook repetido** con la misma `Idempotency-Key`: responde **`200`**, con la misma
   operación, `"replayed": true` y `succeeded`. Sigue habiendo 1 solicitud y 1 efecto.

**PowerShell:** cargar las variables con
`Get-Content .env | ? { $_ -match '^[A-Z_]+=' } | % { $k,$v = $_ -split '=',2; Set-Item "env:$k" $v }`
y usar `curl.exe` con `$env:ADMIN_API_TOKEN`, etcétera. La opción más simple es correr
`uv run python scripts/smoke_compose.py`, que ejecuta exactamente estos pasos y los verifica.

### Qué garantiza esta demo (y qué no)

- Lo que se demuestra: si el proveedor **conserva el efecto y lo informa por `lookup`**, un
  timeout posterior al efecto se resuelve **sin reenviar**: 1 solicitud, 1 efecto, 0 duplicados
  y estado `succeeded`.
- La garantía de un solo efecto **depende del receptor**: de que `lookup` sea fiel y de que
  respete la `idempotency_key`. Si el proveedor no ofrece `lookup` ni idempotencia, el motor no
  reenvía y deja la operación en `dead_letter` para revisión humana. Ver
  `test_uncertain_outcome_without_idempotency_or_lookup_goes_to_dead_letter`.
- No es *exactly-once* de extremo a extremo frente a un receptor que mienta, que expire sus
  claves o que procese un pedido demorado después de responder "no encontrado" sin idempotencia.

---

## 7. Verificaciones

### Tests (sin Docker)

```bash
uv run pytest           # incluye los tests de la Etapa 3
uv run ruff check . && uv run ruff format --check .
```

| Archivo | Qué verifica |
|---|---|
| `tests/integration/test_security.py` | Valores por defecto seguros; combinaciones inválidas que impiden el arranque; matriz de tokens (`401`/`403`); `scenario` rechazado fuera de `local` y campos extra rechazados en el registro; superficie de producción; presencia y ausencia de `/simulator/*` según el entorno; CLI `fault-plan` bloqueado. |
| `tests/integration/test_n8n_flows.py` | Los caminos HTTP de ambos workflows; `pending` abandonada y recuperada; webhook duplicado; carrera intake/worker (también con hilos); evidencia completa de la reconciliación; `dead_letter` con motivo y sin reabrir la recuperación. |
| `tests/unit/test_workflow_definitions.py` | JSON importables; solo nodos de orquestación; URLs `http://engine:8000`; credenciales referenciadas y no embebidas; `Idempotency-Key` tomada del webhook y nunca generada; intake sin token admin ni `scenario`, que rechaza campos extra y responde `409` ante `dead_letter`; compose con puertos en `127.0.0.1` y n8n sin acceso a las bases ni a los tokens. |
| `tests/unit/test_simulator_client_plan.py` | Planes de fallos por clave del cliente. |

### Smoke de Docker Compose

```bash
uv run python scripts/smoke_compose.py          # agrega --down para bajar el stack al terminar
```

El script verifica, sin imprimir secretos:
- exposición: todos los puertos publicados en `127.0.0.1`;
- aislamiento: n8n solo monta `/home/node/.n8n` y `/workflows`, no ve archivos `engine.db*` y
  no recibe los tokens del motor por entorno;
- autenticación: la matriz completa de tokens, `scenario` rechazado en `demo` (motor `403`,
  webhook `400 unexpected_fields` sin registrar la operación), y webhook sin secreto, con
  secreto incorrecto o sin clave;
- la demo de la sección 6, paso a paso;
- una operación `pending` abandonada que el Workflow 2 recupera, y el webhook duplicado;
- una operación agotada en `dead_letter`: el webhook repetido responde `409` con el motivo;
- secretos:
  - logs de ambos contenedores;
  - workflows y credenciales exportados desde n8n (`export:workflow`, `export:credentials`);
  - base de n8n: los tokens del motor y la clave de cifrado nunca en claro, ni siquiera como
    bytes residuales; el secreto del webhook purgado de los datos de ejecución;
  - archivos versionables del repo.

---

## 8. Riesgos y mitigaciones

| Riesgo | Mitigación | Residual |
|---|---|---|
| Endpoints administrativos expuestos | Token admin obligatorio, comparación en tiempo constante, arranque que falla cerrado, puertos en `127.0.0.1` | Cualquiera con el token admin puede forzar corridas del worker (es su propósito). |
| Inyección de fallos en producción | Prohibida por validación; `scenario` en `execute` solo en `local`; ninguna ruta `/simulator/*` montada | — |
| El cliente del webhook elige fallos | El webhook rechaza campos extra (`400 unexpected_fields`); el motor rechaza campos extra en el registro (`422`) y `scenario` en `execute` fuera de `local` (`403`) | — |
| Exposición fuera de la máquina | Puertos en `127.0.0.1` | Header Auth y Bearer compartidos, sin TLS: para uso externo se requieren TLS y controles adicionales. |
| Claves idempotentes generadas por n8n | La clave sale del header del cliente; un test estático prohíbe `$now`, `uuid`, `Math.random`, etc. | El cliente debe reutilizar su clave al reintentar. |
| Reintentos de nodos HTTP | Solo en endpoints idempotentes (`POST /operations`, `GET`); `execute` y `/worker/run` sin reintentos | — |
| Secretos en el repo o en los workflows | `.env` ignorado; `.env.example` con placeholders; credenciales por referencia; tests y smoke lo verifican | — |
| Secreto del webhook en los datos de ejecución de n8n | El intake no guarda ejecuciones y compose fuerza el borrado físico en alrededor de 1 minuto (`EXECUTIONS_DATA_HARD_DELETE_BUFFER=0`) | Durante ese minuto la fila existe en la base de n8n (volumen local). Las páginas libres de SQLite pueden conservar bytes hasta que se reutilicen o hasta el `VACUUM` al reiniciar. |
| n8n accede a SQLite del motor | Sin montajes compartidos; solo HTTP | — |
| Corrupción o bloqueo de SQLite | Un único proceso uvicorn; volumen con nombre | SQLite serializa escrituras (MVP). |
| Lease más corto que una llamada externa | `LEASE_TTL_SECONDS=30` contra un timeout de proveedor simulado inmediato | Sin heartbeat (limitación de la Etapa 2). |
| Imagen de n8n mutable | Tag fijado (`2.47.4`) | Actualizar conscientemente. |

## 9. Operación

```bash
docker compose ps                                 # estado y healthchecks
docker compose logs -f engine                     # logs del motor (sin secretos)
docker compose down                               # detener (conserva volúmenes)
docker compose down -v                            # detener y BORRAR datos de motor y n8n
docker compose exec engine recovery-engine show <op_id>   # CLI dentro del contenedor
```

Para levantar el mismo stack **sin** inyección de fallos, usar `FAULT_INJECTION_ENABLED=false`
en `.env`. Las rutas `/simulator/fault-plans/*` dejan de existir; `/simulator/*` (solo lectura)
sigue disponible con el token admin porque los endpoints administrativos están habilitados. Con
`APP_ENV=production` ninguna ruta `/simulator/*` se monta.
