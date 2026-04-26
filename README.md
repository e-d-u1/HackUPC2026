# Patch v7: análisis más rápido

Sustituye `app/vision_places.py` por el de este ZIP.

## Qué corrige

- Evita que el análisis tarde varios minutos.
- Si Vision detecta un nombre exacto y se resuelve con Places/Geocoding, devuelve ese resultado rápido sin ejecutar el fallback amplio.
- Limita la comprobación de fotos de Google Places, que era la parte más lenta.
- Evita que los fallbacks genéricos tipo Colosseum/Acropolis aparezcan cuando ya se ha detectado un lugar exacto.

## Variables opcionales en `.env`

```txt
LOCATION_FAST_MODE=1
MAX_PHOTOS_TO_VERIFY=1
SKIP_PHOTO_CHECK_FOR_EXACT=1
API_TIMEOUT_SECONDS=12
PHOTO_TIMEOUT_SECONDS=8
MAX_BROAD_QUERIES=12
```

Para máxima velocidad:

```txt
VERIFY_PLACE_PHOTOS=0
```

Para más precisión, pero más lento:

```txt
SKIP_PHOTO_CHECK_FOR_EXACT=0
MAX_PHOTOS_TO_VERIFY=2
```

El error `Failed to fetch` normalmente aparece porque se para el servidor mientras el navegador espera la respuesta, no porque el navegador tenga un bug propio.
