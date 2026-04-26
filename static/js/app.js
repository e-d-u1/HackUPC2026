/* ============================================================
   View it, Visit it — SPA Application Logic
   State machine, API calls, MediaRecorder, Leaflet map
   ============================================================ */

(function () {
  "use strict";

  // ---- State ----
  const state = {
    phase: 1,
    imageFile: null,
    origin: null,
    locations: [],
    confirmedLocations: [],
    flightResults: null,
    mediaRecorder: null,
    audioChunks: [],
    isRecording: false,
  };

  // ---- DOM References ----
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  // Phases
  const phase1 = $("#phase1");
  const phase2 = $("#phase2");
  const phase3 = $("#phase3");

  // Phase 1
  const dropzone = $("#dropzone");
  const fileInput = $("#fileInput");
  const urlInput = $("#urlInput");
  const imagePreview = $("#imagePreview");
  const videoPreview = $("#videoPreview");
  const dropzoneIcon = $("#dropzoneIcon");
  const dropzoneText = $("#dropzoneText");
  const dropzoneHint = $("#dropzoneHint");
  const btnAnalyze = $("#btnAnalyze");
  const originBadge = $("#originBadge");
  const originCity = $("#originCity");

  // Phase 2
  const locationsGrid = $("#locationsGrid");
  const micBtn = $("#micBtn");
  const micStatus = $("#micStatus");
  const transcriptBox = $("#transcriptBox");
  const transcriptText = $("#transcriptText");
  const btnConfirm = $("#btnConfirm");

  // Phase 3
  const destPanel = $("#destPanel");
  const destPanelClose = $("#destPanelClose");
  const btnRestart = $("#btnRestart");

  // Loading
  const loadingOverlay = $("#loadingOverlay");
  const loadingText = $("#loadingText");

  // Step indicators
  const steps = $$(".step-indicator");
  const connectors = $$(".step-connector");

  // ---- Leaflet Map ----
  let map = null;
  let mapLayers = null;

  function initMap() {
    if (map) {
      map.remove();
      map = null;
    }
    map = L.map("flight-map", {
      zoomControl: true,
      attributionControl: false,
    }).setView([30, 10], 3);

    // Dark tile layer
    L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      subdomains: 'abcd',
      maxZoom: 20
    }).addTo(map);

    mapLayers = L.layerGroup().addTo(map);
  }

  // ---- Phase Management ----
  function setPhase(n) {
    state.phase = n;

    [phase1, phase2, phase3].forEach((el, i) => {
      el.classList.toggle("active", i + 1 === n);
    });

    steps.forEach((step, i) => {
      step.classList.remove("active", "completed");
      if (i + 1 === n) step.classList.add("active");
      else if (i + 1 < n) step.classList.add("completed");
    });

    connectors.forEach((conn, i) => {
      conn.classList.toggle("completed", i + 1 < n);
    });

    // Initialize map when entering phase 3
    if (n === 3) {
      setTimeout(() => {
        initMap();
        renderFlightDashboard();
      }, 100);
    }
  }

  // ---- Show/hide loading ----
  function showLoading(text) {
    loadingText.textContent = text;
    loadingOverlay.classList.add("visible");
  }

  function hideLoading() {
    loadingOverlay.classList.remove("visible");
  }

  // ---- Wikipedia image helper ----
  async function getWikiImage(city) {
    async function fetchFromWiki(lang) {
      const url = `https://${lang}.wikipedia.org/w/api.php?action=query&format=json&origin=*&prop=pageimages&titles=${encodeURIComponent(city)}&pithumbsize=800&redirects=1`;
      try {
        const res = await fetch(url);
        const data = await res.json();
        const pages = data.query.pages;
        const pageId = Object.keys(pages)[0];
        if (pageId !== "-1" && pages[pageId].thumbnail) {
          return pages[pageId].thumbnail.source;
        }
      } catch (e) {
        console.warn(`Wiki image error (${lang}):`, e);
      }
      return null;
    }

    let imageUrl = await fetchFromWiki("es");
    if (!imageUrl) imageUrl = await fetchFromWiki("en");
    return (
      imageUrl ||
      `https://placehold.co/800x400/1a1a2e/667eea?text=${encodeURIComponent(city)}`
    );
  }

  // ============================================================
  // PHASE 1: Image Upload & Analysis
  // ============================================================

  // Detect origin on load
  async function detectOrigin() {
    try {
      const res = await fetch("/api/detect-origin");
      const data = await res.json();
      state.origin = data;
      originCity.textContent = `${data.city}, ${data.country}`;
      originBadge.style.display = "inline-flex";
    } catch (e) {
      console.warn("Origin detection failed:", e);
      state.origin = {
        city: "Barcelona",
        country: "Spain",
        latitude: 41.3874,
        longitude: 2.1686,
      };
      originCity.textContent = "Barcelona, Spain";
      originBadge.style.display = "inline-flex";
    }
  }

  // Drag & drop
  dropzone.addEventListener("dragover", (e) => {
    e.preventDefault();
    dropzone.classList.add("drag-over");
  });

  dropzone.addEventListener("dragleave", () => {
    dropzone.classList.remove("drag-over");
  });

  function handleMedia(file, url) {
    state.imageFile = undefined;
    state.imageUrl = undefined;
    
    // Hide placeholders
    dropzoneIcon.style.display = "none";
    dropzoneText.style.display = "none";
    dropzoneHint.style.display = "none";
    dropzone.classList.add("has-image");

    if (file) {
      state.imageFile = file;
      if (file.type.startsWith("video/")) {
        imagePreview.style.display = "none";
        imagePreview.src = "";
        videoPreview.style.display = "block";
        videoPreview.src = URL.createObjectURL(file);
      } else {
        videoPreview.style.display = "none";
        videoPreview.src = "";
        imagePreview.style.display = "block";
        const reader = new FileReader();
        reader.onload = (e) => { imagePreview.src = e.target.result; };
        reader.readAsDataURL(file);
      }
      urlInput.value = "";
    } else if (url) {
      state.imageUrl = url;
      fileInput.value = "";
      if (url.match(/\.(mp4|webm|mov)$/i)) {
        imagePreview.style.display = "none";
        imagePreview.src = "";
        videoPreview.style.display = "block";
        videoPreview.src = url;
      } else {
        videoPreview.style.display = "none";
        videoPreview.src = "";
        imagePreview.style.display = "block";
        imagePreview.src = url;
      }
    }
    btnAnalyze.classList.add("visible");
  }

  // Handle URL change
  urlInput.addEventListener("input", (e) => {
    if (e.target.value.trim().length > 5) {
      handleMedia(null, e.target.value.trim());
    }
  });

  dropzone.addEventListener("drop", (e) => {
    e.preventDefault();
    dropzone.classList.remove("drag-over");
    if (e.dataTransfer.files.length > 0) {
      handleMedia(e.dataTransfer.files[0], null);
    }
  });

  fileInput.addEventListener("change", (e) => {
    if (e.target.files.length > 0) {
      handleMedia(e.target.files[0], null);
    }
  });

  // Analyze button
  btnAnalyze.addEventListener("click", async () => {
    if (!state.imageFile && !state.imageUrl) return;

    btnAnalyze.disabled = true;
    showLoading("Analizando contenido con IA...");

    try {
      const formData = new FormData();
      if (state.imageFile) formData.append("media", state.imageFile);
      if (state.imageUrl) formData.append("url", state.imageUrl);

      const res = await fetch("/api/analyze-media", { method: "POST", body: formData });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || "Error analyzing image");
      }

      const data = await res.json();
      state.locations = data.locations || [];

      if (state.locations.length === 0) {
        alert("No se pudieron detectar ubicaciones en la imagen. Intenta con otra imagen.");
        btnAnalyze.disabled = false;
        hideLoading();
        return;
      }

      hideLoading();
      setPhase(2);
      renderLocationCards();
    } catch (e) {
      console.error("Analysis error:", e);
      alert("Error al analizar la imagen: " + e.message);
      hideLoading();
      btnAnalyze.disabled = false;
    }
  });

  // ============================================================
  // PHASE 2: Voice Validation
  // ============================================================

  function renderLocationCards() {
    locationsGrid.innerHTML = "";

    state.locations.forEach((loc, i) => {
      const card = document.createElement("div");
      card.className = "location-card";
      card.innerHTML = `
        <div class="location-card-city">${loc.city}</div>
        <div class="location-card-country">${loc.country}</div>
        <div class="location-card-meta">
          <span>🌡️ ${loc.climate || "N/A"}</span>
          <span>🏔️ ${loc.landscape || "N/A"}</span>
          <span>📍 ${loc.latitude?.toFixed(2)}, ${loc.longitude?.toFixed(2)}</span>
        </div>
        <div class="confidence-bar">
          <div class="confidence-fill" style="width: ${((loc.confidence || 0) * 100).toFixed(0)}%"></div>
        </div>
      `;
      locationsGrid.appendChild(card);
    });
  }

  // Microphone recording — Web Speech API
  const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  let recognition = null;

  micBtn.addEventListener("click", () => {
    if (state.isRecording) {
      stopRecording();
    } else {
      startRecording();
    }
  });

  function startRecording() {
    if (!SpeechRecognition) {
      alert("Tu navegador no soporta reconocimiento de voz. Usa Chrome o Edge.");
      return;
    }

    recognition = new SpeechRecognition();
    recognition.lang = "es-ES";
    recognition.interimResults = false;
    recognition.maxAlternatives = 1;

    recognition.onresult = async (event) => {
      const transcript = event.results[0][0].transcript;
      await sendTranscriptToBackend(transcript);
    };

    recognition.onerror = (event) => {
      console.error("Speech error:", event.error);
      alert("Error al reconocer voz: " + event.error);
      resetMicUI();
    };

    recognition.onend = () => {
      state.isRecording = false;
      resetMicUI();
    };

    recognition.start();
    state.isRecording = true;
    micBtn.classList.add("recording");
    micBtn.innerHTML = "⏹";
    micStatus.textContent = "Escuchando... Pulsa para detener";
    micStatus.className = "mic-status recording";
  }

  function stopRecording() {
    if (recognition) {
      recognition.stop();
    }
    state.isRecording = false;
    resetMicUI();
  }

  function resetMicUI() {
    micBtn.classList.remove("recording");
    micBtn.innerHTML = "🎙️";
    micStatus.textContent = "Pulsa para grabar";
    micStatus.className = "mic-status";
  }

  async function sendTranscriptToBackend(transcript) {
    showLoading("Refinando destinos...");

    try {
      const formData = new FormData();
      formData.append("transcript", transcript);
      formData.append("locations", JSON.stringify(state.locations));

      const res = await fetch("/api/voice-validate", {
        method: "POST",
        body: formData,
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || "Error processing voice");
      }

      const data = await res.json();

      transcriptText.textContent = data.transcript || "(sin transcripción)";
      transcriptBox.classList.add("visible");

      if (data.locations && data.locations.length > 0) {
        state.locations = data.locations;
        renderLocationCards();
      }

      micStatus.textContent = "✅ Listo — destinos actualizados";
      micStatus.className = "mic-status";
      hideLoading();
    } catch (e) {
      console.error("Voice validation error:", e);
      alert("Error al procesar el audio: " + e.message);
      hideLoading();
    }
  }

  // Confirm button → Phase 3
  btnConfirm.addEventListener("click", async () => {
    if (state.locations.length === 0) return;

    state.confirmedLocations = [...state.locations];
    btnConfirm.disabled = true;

    // Build destinations string
    const destCities = state.confirmedLocations.map((l) => l.city).join(",");
    const originCity = state.origin?.city || "Barcelona";

    showLoading("Buscando vuelos baratos...");

    try {
      const res = await fetch(
        `/api/search-flights?origin=${encodeURIComponent(originCity)}&destinations=${encodeURIComponent(destCities)}&date=2026`
      );

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || "Error searching flights");
      }

      state.flightResults = await res.json();
      hideLoading();
      setPhase(3);
    } catch (e) {
      console.error("Flight search error:", e);
      alert("Error al buscar vuelos: " + e.message);
      hideLoading();
      btnConfirm.disabled = false;
    }
  });

  // ============================================================
  // PHASE 3: Flight Dashboard
  // ============================================================

  async function renderFlightDashboard() {
    if (!state.flightResults || !map) return;

    const data = state.flightResults;
    mapLayers.clearLayers();

    // Origin marker
    const originLat = data.origin?.lat || state.origin?.latitude || 41.39;
    const originLon = data.origin?.lon || state.origin?.longitude || 2.17;
    const originName = data.origin?.name || state.origin?.city || "Origen";

    const originIcon = L.divIcon({
      className: "",
      html: `<div class="price-marker origin">📍 ${originName}</div>`,
      iconSize: [0, 0],
      iconAnchor: [0, 0],
    });

    L.marker([originLat, originLon], { icon: originIcon }).addTo(mapLayers);

    const bounds = [[originLat, originLon]];

    // Destination markers
    const destNames = Object.keys(data.results || {});

    for (const dest of destNames) {
      const info = data.results[dest];
      if (!info.lat && !info.lon) continue;

      const priceLabel = info.price || "N/A";

      const destIcon = L.divIcon({
        className: "",
        html: `<div class="price-marker">${priceLabel}</div>`,
        iconSize: [0, 0],
        iconAnchor: [0, 0],
      });

      // Find matching confirmed location for extra data
      const locData = state.confirmedLocations.find(
        (l) => l.city.toLowerCase() === dest.toLowerCase()
      );

      const marker = L.marker([info.lat, info.lon], { icon: destIcon }).addTo(
        mapLayers
      );

      // Click → open side panel
      marker.on("click", () =>
        openDestPanel(dest, info, locData)
      );

      // Dashed line
      L.polyline([[originLat, originLon], [info.lat, info.lon]], {
        color: "#667eea",
        weight: 2,
        opacity: 0.5,
        dashArray: "8, 12",
      }).addTo(mapLayers);

      bounds.push([info.lat, info.lon]);
    }

    // Fit bounds
    if (bounds.length > 1) {
      map.fitBounds(bounds, { padding: [60, 60] });
    }
  }

  async function openDestPanel(destName, flightInfo, locData) {
    const panelImage = $("#destPanelImage");
    const panelCity = $("#destPanelCity");
    const panelCountry = $("#destPanelCountry");
    const panelPrice = $("#destPanelPrice");
    const panelDate = $("#destPanelDate");
    const panelStops = $("#destPanelStops");
    const panelClimate = $("#destPanelClimate");
    const panelLandscape = $("#destPanelLandscape");
    const panelDescription = $("#destPanelDescription");
    const panelHotel = $("#destPanelHotel");

    // Fill data
    panelCity.textContent = destName;
    panelCountry.textContent = locData?.country || "";
    panelPrice.textContent = flightInfo.price || "N/A";
    panelDate.textContent = flightInfo.date || "N/A";
    panelStops.textContent = flightInfo.stops || "N/A";
    panelClimate.textContent = locData?.climate || "N/A";
    panelLandscape.textContent = locData?.landscape || "N/A";
    panelDescription.textContent = locData?.description || "Destino de viaje";
    panelHotel.textContent = flightInfo.hotel_price || "N/A";

    // Fetch image
    const imgUrl = await getWikiImage(destName);
    panelImage.src = imgUrl;
    panelImage.onerror = () => {
      panelImage.src = `https://placehold.co/800x400/1a1a2e/667eea?text=${encodeURIComponent(destName)}`;
    };

    destPanel.classList.add("open");
  }

  // Close panel
  destPanelClose.addEventListener("click", () => {
    destPanel.classList.remove("open");
  });

  // Restart button
  btnRestart.addEventListener("click", () => {
    // Reset state
    state.imageFile = undefined;
    state.imageUrl = undefined;
    state.locations = [];
    state.confirmedLocations = [];
    state.flightResults = null;

    // Reset UI
    imagePreview.style.display = "none";
    imagePreview.src = "";
    videoPreview.style.display = "none";
    videoPreview.src = "";
    dropzoneIcon.style.display = "block";
    dropzoneText.style.display = "block";
    dropzoneHint.style.display = "block";
    dropzone.classList.remove("has-image");
    
    urlInput.value = "";
    fileInput.value = "";

    btnAnalyze.classList.remove("visible");
    btnAnalyze.disabled = false;
    btnConfirm.disabled = false;
    transcriptBox.classList.remove("visible");
    micStatus.textContent = "Pulsa para grabar";
    micStatus.className = "mic-status";
    destPanel.classList.remove("open");
    fileInput.value = "";

    setPhase(1);
  });

  // ---- Init ----
  detectOrigin();
  setPhase(1);
})();
