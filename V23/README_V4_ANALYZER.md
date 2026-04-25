# HACKUPC Location Prototype V4

Esta versión permite analizar tanto:

- una URL de imagen o vídeo
- una imagen o vídeo descargado en tu ordenador

El programa genera dos ficheros:

- `output_location.json`: resultado completo, con análisis visual, queries, candidatos, scores y verificación con fotos.
- `output_locations_simple.json`: resultado simplificado para usar desde otra parte de la app.

## Estructura recomendada

Coloca la carpeta del proyecto y una carpeta `media` al mismo nivel:

```text
HackUPC/
├── V3/
│   ├── CMakeLists.txt
│   ├── main.cpp
│   ├── README.md
│   └── build/
└── media/
    ├── foto.jpg
    ├── imagen.png
    └── video.mp4
```

También puedes tener `media` dentro de `V3`, pero los ejemplos están pensados para que `media` esté a la altura de `V3`.

## Compilar en PowerShell

Desde la carpeta del proyecto:

```powershell
cd C:\Users\roger\OneDrive\Escritori\src\HackUPC\V3

mkdir build
cd build

cmake .. -G "MinGW Makefiles" -DCMAKE_PREFIX_PATH=C:/msys64/ucrt64
C:\msys64\ucrt64\bin\mingw32-make.exe
```

## Variables de entorno

```powershell
$env:GOOGLE_VISION_API_KEY="TU_API_KEY"
$env:GOOGLE_MAPS_API_KEY="TU_API_KEY"
```

También puedes usar una sola key para todo:

```powershell
$env:GOOGLE_API_KEY="TU_API_KEY"
```

## Ejecutar con una URL

```powershell
.\location_prototype.exe "https://example.com/foto.jpg" --max-candidates 5 --photos-per-place 1
```

## Ejecutar con una imagen local

Si estás dentro de `V3/build` y tu carpeta `media` está a la altura de `V3`, usa:

```powershell
.\location_prototype.exe "..\..\media\foto.jpg" --max-candidates 5 --photos-per-place 1
```

## Ejecutar con un vídeo local

```powershell
.\location_prototype.exe "..\..\media\video.mp4" --max-candidates 5 --photos-per-place 1
```

El programa detecta si es imagen o vídeo por la extensión del archivo. Para vídeo, extrae 3 frames al 25%, 50% y 75% de duración.

## Formato simplificado

`output_locations_simple.json` tiene este formato:

```json
{
  "source_input": "..\\..\\media\\foto.jpg",
  "source_type": "local_file",
  "media_type": "image",
  "exact_location_found": false,
  "confidence_level": "medium",
  "possible_locations": [
    {
      "name": "Great Sand Dunes National Park and Preserve",
      "formatted_address": "Colorado, United States",
      "coordinates": {
        "latitude": 37.7916,
        "longitude": -105.5943
      }
    }
  ]
}
```

## Nota

`final_confidence` en el JSON completo es un score heurístico, no una probabilidad matemática exacta.
