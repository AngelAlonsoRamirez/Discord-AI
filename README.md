# 🚀 Turbo Vieja — Bot de Discord con memoria comunitaria e IA

Este proyecto es un **proyecto personal desarrollado para una comunidad de Discord**, diseñado para ofrecer una experiencia más humana, interactiva y dinámica dentro del servidor.

Turbo Vieja no es un bot típico:  
aprende de la comunidad, recuerda contexto y responde con personalidad gamer usando IA.

---

## 📂 Archivos principales

- `bot.py` → Código principal del bot (documentado).
- `config.json` → Configuración externa del bot.
- `bot_memory.db` → Base de datos SQLite (se genera automáticamente).

---

## 🧠 Qué hace el bot

1. Carga la configuración desde `config.json`.
2. Valida tokens, API keys y parámetros críticos.
3. Se conecta a Discord usando `discord.py`.
4. Aprende del comportamiento de usuarios y canales.
5. Guarda historial en SQLite.
6. Responde:
   - En canales definidos
   - O cuando se le menciona
7. Permite consultas de historial (solo roles autorizados).
8. Genera respuestas con IA (OpenRouter).
9. Rota API keys automáticamente si fallan.
10. Protege datos sensibles y evita fugas de información.

---

## ⚙️ Requisitos

- Python **3.10+ recomendado**
- Bot creado en Discord Developer Portal
- Intents activados:
  - ✅ Server Members Intent
  - ✅ Message Content Intent
- API Key de OpenRouter

---

## 🧪 Instalación

```bash
mkdir turbovieja
cd turbovieja

python3 -m venv venv
source venv/bin/activate

pip install -U pip
pip install discord.py aiohttp
```

Si usas Python 3.8:

```bash
pip install backports.zoneinfo
```

---

## 🔧 Configuración

Edita `config.json`:

```json
"TOKEN": "TOKEN_REAL_DEL_BOT",

"OPENROUTER_API_KEYS": [
  "sk-or-v1-xxxx"
],

"ALLOWED_GUILD_IDS": [
  ID_DEL_SERVIDOR
],

"OWNER_DISCORD_ID": ID_DEL_OWNER,

"ADMIN_LOOKUP_ROLE_IDS": [
  ID_ROL_ADMIN_1,
  ID_ROL_ADMIN_2
],

"REPLY_CHANNEL_IDS": [
  ID_CANAL_1,
  ID_CANAL_2
]
```

---

## ▶️ Ejecución manual

```bash
python3 bot.py
```

---

## 💬 Comandos internos

### `!reset`
Limpia la memoria temporal del canal  
🔒 Solo admin u owner

### `!perfil`
Muestra el perfil aprendido del usuario

### `!canal`
Muestra el perfil del canal

### `!keys`
Estado de las API keys (sin mostrar claves)

---

## 🔍 Consultas de historial

🔒 Solo disponibles para roles definidos en `ADMIN_LOOKUP_ROLE_IDS`

Ejemplos:

```text
qué dijo @usuario hace 10 minutos
últimos 20 mensajes de @usuario
resumen de lo que se está hablando en #general de los últimos 30 minutos
```

---

## 🛡️ Seguridad incluida

- ✔ Whitelist de servidores
- ✔ Rate limit por usuario
- ✔ Validación de base de datos
- ✔ Sanitizado de texto
- ✔ Protección contra prompt injection
- ✔ Logs sin datos sensibles

---

## 🧠 Base de datos

- `user_profiles`
- `messages`
- `channel_profiles`
- `user_interactions`

---

## ⚙️ systemd (recomendado)

Archivo:

```
/etc/systemd/system/turbovieja.service
```

Config:

```
[Unit]
Description=Bot Discord Turbo Vieja
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/home/usuario/turbovieja
ExecStart=/home/usuario/turbovieja/venv/bin/python /home/usuario/turbovieja/bot.py
Restart=always
User=usuario

[Install]
WantedBy=multi-user.target
```

---

## 🔧 Mantenimiento

Logs:
```
journalctl -u turbovieja -n 50 --no-pager
```

Reiniciar:
```
sudo systemctl restart turbovieja
```

---

## ⚠️ Notas importantes

- ❌ No subas `config.json` real a GitHub
- ❌ No compartas la base de datos
- ✔ Usa varias API keys si puedes
- ✔ Verifica intents en Discord
