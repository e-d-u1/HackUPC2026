# View it, Visit it — Multi-Modal Travel App (HackUPC 2026)

**View it, Visit it** es una aplicación web de viajes de vanguardia que utiliza Inteligencia Artificial multimodal y validación por voz para ayudarte a descubrir tu próximo destino. El proyecto está dividido en tres fases lógicas integradas en una experiencia de Single Page Application (SPA) premium.

## 🌟 Características Principales

1.  **Fase 1: Análisis de Imagen y Origen (Vision & Metadata)**
    *   Subida de imagen para identificar destinos probables mediante **Gemini 2.5 Flash**.
    *   Detección automática de la ciudad de origen del usuario mediante geolocalización por IP.
2.  **Fase 2: Validación y Corrección (Voice UI & Human-in-the-Loop)**
    *   Interfaz moderna para revisar las sugerencias de la IA.
    *   Validación por voz: Corrige o confirma destinos hablando directamente al navegador (transcripción vía **ElevenLabs** y refinamiento con LLM).
3.  **Fase 3: Dashboard de Vuelos y Mapa Interactivo**
    *   Búsqueda de vuelos en tiempo real mediante la API de **Skyscanner**.
    *   Mapa interactivo con marcadores de precio y arcos de ruta.
    *   Panel lateral con imágenes dinámicas de **Wikipedia** y detalles del destino (clima, paisajes, hoteles).

---

## 🚀 Guía de Inicio Rápido

Sigue estos pasos para ejecutar la aplicación en tu entorno local (Linux/macOS/Windows).

### 1. Requisitos Previos

Necesitarás las siguientes API Keys configuradas en tu archivo `.env`:
*   **Gemini API Key:** Para el análisis multimodal de imágenes y refinamiento de texto.
*   **ElevenLabs API Key:** Para la transcripción de voz a texto (Scribe v1).
*   **Skyscanner API Key:** Para obtener ofertas de vuelos reales.

### 2. Instalación

1.  **Clonar el repositorio y entrar en la carpeta:**
    ```bash
    cd HackUPC2026
    ```

2.  **Crear y activar el entorno virtual:**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # En Windows: .venv\Scripts\activate
    ```

3.  **Instalar dependencias:**
    ```bash
    pip install -r requirements.txt
    ```

### 3. Configuración del Archivo `.env`

Crea un archivo `.env` en la raíz del proyecto (o edita el existente) con el siguiente formato:

```text
GEMINI_API_KEY=tu_clave_gemini
ELEVENLABS_API_KEY=tu_clave_elevenlabs
SKYSCANNER_API_KEY=tu_clave_skyscanner
```

### 4. Ejecución del Servidor

Levanta el servidor unificado de FastAPI ejecutando:

```bash
python main.py
```

El servidor estará disponible en: **`http://localhost:7999`**

---

## 🛠️ Tecnologías Utilizadas

*   **Backend:** FastAPI (Python), Google GenAI SDK (Gemini), ElevenLabs SDK.
*   **Frontend:** Vanilla JS (SPA architecture), CSS Moderno (Glassmorphism), Leaflet.js (Mapas).
*   **APIs Externas:** Skyscanner Partners API, Wikipedia API, ipapi.co.

## 📁 Estructura del Código

*   `main.py`: Punto de entrada unificado y configuración de FastAPI.
*   `app/main.py`: Definición de todos los endpoints de la API (Fases 1, 2 y 3).
*   `app/llm.py`: Lógica de IA para análisis de imágenes y corrección por voz.
*   `app/stt.py`: Integración con ElevenLabs para Speech-to-Text.
*   `static/`: Contiene el sistema de diseño CSS y la lógica JS de la SPA.
*   `templates/`: Plantilla HTML base de la aplicación.
