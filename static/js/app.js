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
  const imagePreview = $("#imagePreview");
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
    L.tileLayer(
      "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
      {
        maxZoom: 19,
      }
    ).addTo(map);

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

  dropzone.addEventListener("drop", (e) => {
    e.preventDefault();
    dropzone.classList.remove("drag-over");
    if (e.dataTransfer.files.length > 0) {
      handleFile(e.dataTransfer.files[0]);
    }
  });

  fileInput.addEventListener("change", (e) => {
    if (e.target.files.length > 0) {
      handleFile(e.target.files[0]);
    }
  });

  function handleFile(file) {
    if (!file.type.startsWith("image/")) {
      alert("Por favor, selecciona una imagen.");
      return;
    }

    state.imageFile = file;

    const reader = new FileReader();
    reader.onload = (e) => {
      imagePreview.src = e.target.result;
      imagePreview.classList.add("visible");
      dropzone.classList.add("has-image");
      btnAnalyze.classList.add("visible");
    };
    reader.readAsDataURL(file);
  }

  // Analyze button
  btnAnalyze.addEventListener("click", async () => {
    if (!state.imageFile) return;

    btnAnalyze.disabled = true;
    showLoading("Analizando imagen con IA...");

    try {
      const formData = new FormData();
      formData.append("image", state.imageFile);

      const res = await fetch("/api/analyze-image", { method: "POST", body: formData });

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

  // Microphone recording
  micBtn.addEventListener("click", async () => {
    if (state.isRecording) {
      stopRecording();
    } else {
      startRecording();
    }
  });

  async function startRecording() {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      state.audioChunks = [];
      state.mediaRecorder = new MediaRecorder(stream, {
        mimeType: MediaRecorder.isTypeSupported("audio/webm;codecs=opus")
          ? "audio/webm;codecs=opus"
          : "audio/webm",
      });

      state.mediaRecorder.ondataavailable = (e) => {
        if (e.data.size > 0) state.audioChunks.push(e.data);
      };

      state.mediaRecorder.onstop = () => {
        stream.getTracks().forEach((t) => t.stop());
        processAudioRecording();
      };

      state.mediaRecorder.start();
      state.isRecording = true;
      micBtn.classList.add("recording");
      micBtn.innerHTML = "⏹";
      micStatus.textContent = "Grabando... Pulsa para detener";
      micStatus.className = "mic-status recording";
    } catch (e) {
      console.error("Mic error:", e);
      alert(
        "No se pudo acceder al micrófono. Asegúrate de dar permiso."
      );
    }
  }

  function stopRecording() {
    if (state.mediaRecorder && state.mediaRecorder.state === "recording") {
      state.mediaRecorder.stop();
      state.isRecording = false;
      micBtn.classList.remove("recording");
      micBtn.innerHTML = "🎙️";
      micStatus.textContent = "Procesando audio...";
      micStatus.className = "mic-status processing";
    }
  }

  async function processAudioRecording() {
    const blob = new Blob(state.audioChunks, { type: "audio/webm" });

    showLoading("Transcribiendo y refinando destinos...");

    try {
      const formData = new FormData();
      formData.append("audio", blob, "recording.webm");
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

      // Show transcript
      transcriptText.textContent = data.transcript || "(sin transcripción)";
      transcriptBox.classList.add("visible");

      // Update locations
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
      micStatus.textContent = "Pulsa para grabar";
      micStatus.className = "mic-status";
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
        html: `<div class="price-marker">✈️ ${priceLabel}</div>`,
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
    state.imageFile = null;
    state.locations = [];
    state.confirmedLocations = [];
    state.flightResults = null;

    // Reset UI
    imagePreview.classList.remove("visible");
    imagePreview.src = "";
    dropzone.classList.remove("has-image");
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
