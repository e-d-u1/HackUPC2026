# HACKUPC Location Prototype V5

Esta versión permite analizar:

- una URL de imagen o vídeo
- una imagen o vídeo descargado en tu ordenador

Cambios principales de esta versión:

- Se elimina `output_locations_simple.json`.
- Solo se genera `output_location.json`.
- Se añade un filtro para no rellenar resultados con candidatos claramente peores cuando el primer lugar destaca mucho.
- Se mejora el fallback para vídeos: si los 3 frames no dan pistas muy claras, el programa genera queries genéricas con las mejores pistas visuales para intentar devolver lugares posibles igualmente.

## Estructura recomendada

Coloca la carpeta del proyecto y una carpeta `media` al mismo nivel:

```text
HackUPC/
├── V5/
│   ├── CMakeLists.txt
│   ├── main.cpp
│   ├── README.md
│   └── build/
└── media/
    ├── foto.jpg
    ├── imagen.png
    └── video.mp4
```

## Compilar en PowerShell

Desde la carpeta del proyecto:

```powershell
cd C:\Users\roger\OneDrive\Escritori\src\HackUPC\V5

mkdir build
cd build

cmake .. -G "MinGW Makefiles" -DCMAKE_PREFIX_PATH=C:/msys64/ucrt64
C:\msys64\ucrt64\bin\mingw32-make.exe
```

Si ya existe `build`, puedes borrar y recompilar:

```powershell
cd C:\Users\roger\OneDrive\Escritori\src\HackUPC\V5

Remove-Item -Recurse -Force build
mkdir build
cd build

cmake .. -G "MinGW Makefiles" -DCMAKE_PREFIX_PATH=C:/msys64/ucrt64
C:\msys64\ucrt64\bin\mingw32-make.exe
```

## Variables de entorno

Puedes usar una clave para Vision y otra para Maps/Places:

```powershell
$env:GOOGLE_VISION_API_KEY="TU_API_KEY_DE_VISION"
$env:GOOGLE_MAPS_API_KEY="TU_API_KEY_DE_MAPS_PLACES"
```

O una misma clave para ambas:

```powershell
$env:GOOGLE_API_KEY="TU_API_KEY"
```

Necesitas tener activadas estas APIs en Google Cloud:

- Cloud Vision API
- Places API

## Ejecutar con una imagen local

Si `media` está al mismo nivel que `V5`:

```powershell
.\location_prototype.exe "..\..\media\foto.jpg" --max-candidates 5 --photos-per-place 1
```

## Ejecutar con un vídeo local

```powershell
.\location_prototype.exe "..\..\media\video.mp4" --max-candidates 5 --photos-per-place 1
```

## Ejecutar con URL

```powershell
.\location_prototype.exe "https://example.com/foto.jpg" --max-candidates 5 --photos-per-place 1
```

## Resultado

El programa genera únicamente:

```text
output_location.json
```

Dentro encontrarás:

- `source_input`: URL o ruta local usada.
- `source_type`: `url` o `local_file`.
- `media_type`: `image` o `video`.
- `analyzed_images`: imágenes analizadas. Si es vídeo, serán 3 frames.
- `visual_summary`: resumen combinado de etiquetas, entidades, texto, landmarks y logos.
- `location_inference`: posibles lugares.

## Sobre el filtro de candidatos dominantes

Si el mejor candidato tiene un score claramente superior, el programa elimina opciones mucho peores en vez de rellenar hasta `--max-candidates`.

La regla actual es:

```text
Si el mejor candidato tiene final_confidence >= 0.65,
se eliminan candidatos con diferencia >= 0.22
o con score <= 70% del score del mejor.
```

El propio JSON indica si se ha aplicado el filtro en:

```json
"ranking_filter": {
  "dominant_candidate_filter_applied": true,
  "removed_clearly_worse_candidates": 2
}
```

## Sobre vídeos

Para vídeos, el programa extrae 3 frames:

- 25% del vídeo
- 50% del vídeo
- 75% del vídeo

Si esos frames no generan pistas claras, esta versión intenta igualmente generar posibles lugares usando las mejores etiquetas visuales disponibles, por ejemplo:

```text
desert dune sand tourist attraction
mountain lake scenic location
beach coast travel destination
```

Por eso puede devolver candidatos aunque la confianza sea baja. En esos casos, el resultado debe interpretarse como lugares visualmente parecidos, no como ubicación confirmada.

## Nota importante

`final_confidence` no es una probabilidad matemática real. Es un score heurístico para ordenar candidatos según:

- coincidencias entre entidades de Vision y nombres/direcciones de Places
- similitud visual con fotos del lugar
- fuerza de las pistas visuales disponibles
