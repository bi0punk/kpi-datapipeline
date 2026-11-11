# Política de seguridad

## Cómo reportar

Usa **GitHub Security Advisories** o un issue privado. Incluye versión/commit,
componente, pasos de reproducción e impacto. No adjuntes credenciales reales.

Respuesta objetivo: acuse en 72 h y parche/mitigación en 14 días.

## Alcance

- exposición de `DB_PASSWORD` o `.env`;
- SQL injection en `DB_TABLE` (viene del entorno, no de los mensajes);
- escritura no autorizada en PostgreSQL;
- DoS por mensajes malformados o volumen excesivo;
- dependencias con CVE.

Fuera de alcance: configuración de seguridad de Kafka/PostgreSQL upstream y
despliegues que exponen 9092/5432/8080 a internet sin VPN.

## Recomendaciones

- Kafka en PLAINTEXT es solo para desarrollo; en producción usa SASL_SSL.
- No publiques `9092`, `29092`, `5432` ni `8080` fuera de la red interna.
- Rota `DB_PASSWORD` y `CB_ADMIN_PASSWORD` si se filtraron.
- Mantén dependencias al día: Dependabot + `pip-audit` en CI.
