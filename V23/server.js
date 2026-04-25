require('dotenv').config();

const express = require('express');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');

const app = express();
const PORT = Number(process.env.PORT || 3000);
const ROOT_DIR = __dirname;
const UPLOAD_DIR = path.join(ROOT_DIR, 'uploads');
const OUTPUT_JSON = path.join(ROOT_DIR, 'output_location.json');
fs.mkdirSync(UPLOAD_DIR, { recursive: true });

app.set('trust proxy', true);
app.use(cors());
app.use(express.json({ limit: '2mb' }));
app.use(express.static(path.join(ROOT_DIR, 'public')));

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, UPLOAD_DIR),
  filename: (_req, file, cb) => {
    const safeOriginal = file.originalname.replace(/[^a-zA-Z0-9._-]/g, '_');
    cb(null, `${Date.now()}_${safeOriginal}`);
  }
});

const allowedExt = new Set([
  '.jpg', '.jpeg', '.png', '.webp', '.bmp', '.gif',
  '.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v'
]);

const upload = multer({
  storage,
  limits: { fileSize: 250 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    if (!allowedExt.has(ext)) {
      return cb(new Error('Unsupported format. Use an image or video: jpg, png, webp, mp4, mov, avi, mkv, webm...'));
    }
    cb(null, true);
  }
});

function firstExistingPath(paths) {
  return paths.find(p => p && fs.existsSync(p)) || null;
}

function getExecutablePath() {
  const configured = process.env.LOCATION_EXE;
  if (configured) return path.isAbsolute(configured) ? configured : path.resolve(ROOT_DIR, configured);

  const candidates = ['./location_prototype.exe', './build/location_prototype.exe'].map(p => path.resolve(ROOT_DIR, p));
  return firstExistingPath(candidates) || candidates[0];
}

function readJsonIfExists(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJson(filePath, obj) {
  fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), 'utf8');
}

function cleanIp(ip) {
  if (!ip) return null;
  let value = String(ip).trim();
  if (value.includes(',')) value = value.split(',')[0].trim();
  if (value.startsWith('::ffff:')) value = value.slice(7);
  return value;
}

function isLocalIp(ip) {
  const value = cleanIp(ip);
  if (!value) return true;
  return (
    value === '::1' ||
    value === '127.0.0.1' ||
    value === 'localhost' ||
    value.startsWith('10.') ||
    value.startsWith('192.168.') ||
    /^172\.(1[6-9]|2\d|3[0-1])\./.test(value) ||
    value.startsWith('fe80:') ||
    value.startsWith('fc') ||
    value.startsWith('fd')
  );
}

function getClientIp(req) {
  const forwarded = req.headers['x-forwarded-for'];
  if (forwarded) return cleanIp(forwarded.split(',')[0]);
  return cleanIp(req.headers['x-real-ip'] || req.socket?.remoteAddress || req.connection?.remoteAddress || req.ip || null);
}

async function lookupIpLocation(ip) {
  const clean = cleanIp(ip);
  if (!clean || isLocalIp(clean)) {
    return {
      status: 'skipped',
      reason: 'Local/private IP address. Country cannot be inferred from localhost or a private network IP.',
      provider: null,
      ip_used: clean,
      country: null,
      country_code: null,
      city: null,
      region: null,
      timezone: null
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const response = await fetch(`https://ipapi.co/${encodeURIComponent(clean)}/json/`, {
      signal: controller.signal,
      headers: { 'User-Agent': 'HackUPC-Location-V23/1.0' }
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.error) {
      return {
        status: 'error',
        provider: 'ipapi.co',
        ip_used: clean,
        error: data.reason || data.error || `HTTP ${response.status}`,
        country: null,
        country_code: null,
        city: null,
        region: null,
        timezone: null
      };
    }

    return {
      status: 'ok',
      provider: 'ipapi.co',
      ip_used: clean,
      country: data.country_name || null,
      country_code: data.country_code || null,
      city: data.city || null,
      region: data.region || null,
      timezone: data.timezone || null,
      latitude: data.latitude ?? null,
      longitude: data.longitude ?? null
    };
  } catch (err) {
    return {
      status: 'error',
      provider: 'ipapi.co',
      ip_used: clean,
      error: err.name === 'AbortError' ? 'IP lookup timed out.' : err.message,
      country: null,
      country_code: null,
      city: null,
      region: null,
      timezone: null
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function buildRequestInfo(req, inputMethod, extra = {}) {
  const ip = getClientIp(req);
  const ipLocation = await lookupIpLocation(ip);
  return {
    user_ip: ip,
    user_country: ipLocation.country,
    user_country_code: ipLocation.country_code,
    user_ip_location: ipLocation,
    analyzed_at: new Date().toISOString(),
    input_method: inputMethod,
    ...extra
  };
}

function toNumberOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function validLatLng(lat, lng) {
  return lat !== null && lng !== null && Math.abs(lat) <= 90 && Math.abs(lng) <= 180;
}

function collectCoordinatePairs(obj, depth = 0, pairs = []) {
  if (!obj || typeof obj !== 'object' || depth > 6) return pairs;

  if (Array.isArray(obj)) {
    if (obj.length >= 2) {
      const a = toNumberOrNull(obj[0]);
      const b = toNumberOrNull(obj[1]);
      if (validLatLng(a, b)) pairs.push({ latitude: a, longitude: b, source: 'array_lat_lng' });
      if (validLatLng(b, a)) pairs.push({ latitude: b, longitude: a, source: 'array_lng_lat' });
    }
    obj.forEach(item => collectCoordinatePairs(item, depth + 1, pairs));
    return pairs;
  }

  const latKeys = ['latitude', 'lat', 'latitud'];
  const lngKeys = ['longitude', 'lng', 'lon', 'long', 'longitud'];

  for (const latKey of latKeys) {
    for (const lngKey of lngKeys) {
      if (Object.prototype.hasOwnProperty.call(obj, latKey) && Object.prototype.hasOwnProperty.call(obj, lngKey)) {
        const lat = toNumberOrNull(obj[latKey]);
        const lng = toNumberOrNull(obj[lngKey]);
        if (validLatLng(lat, lng)) pairs.push({ latitude: lat, longitude: lng, source: `${latKey}_${lngKey}` });
      }
    }
  }

  if (Object.prototype.hasOwnProperty.call(obj, 'latitudeE7') && Object.prototype.hasOwnProperty.call(obj, 'longitudeE7')) {
    const lat = toNumberOrNull(obj.latitudeE7);
    const lng = toNumberOrNull(obj.longitudeE7);
    if (lat !== null && lng !== null && validLatLng(lat / 1e7, lng / 1e7)) {
      pairs.push({ latitude: lat / 1e7, longitude: lng / 1e7, source: 'e7' });
    }
  }

  for (const value of Object.values(obj)) collectCoordinatePairs(value, depth + 1, pairs);
  return pairs;
}

function pickBestCoordinatePair(pairs) {
  if (!pairs.length) return { latitude: null, longitude: null };
  const nonZero = pairs.find(p => !(p.latitude === 0 && p.longitude === 0));
  const picked = nonZero || pairs[0];
  return { latitude: picked.latitude, longitude: picked.longitude };
}

function normalizeCoordinates(candidate = {}) {
  const prioritySources = [
    candidate.coordinates,
    candidate.location,
    candidate.geometry?.location,
    candidate.geometry,
    candidate.viewport,
    candidate.raw,
    candidate
  ];

  for (const source of prioritySources) {
    const pairs = collectCoordinatePairs(source);
    const picked = pickBestCoordinatePair(pairs);
    if (picked.latitude !== null && picked.longitude !== null && !(picked.latitude === 0 && picked.longitude === 0)) return picked;
  }

  const allPairs = collectCoordinatePairs(candidate);
  return pickBestCoordinatePair(allPairs);
}

function hasRealCoordinates(candidate) {
  const c = candidate?.coordinates || normalizeCoordinates(candidate);
  return c?.latitude !== null && c?.longitude !== null && !(c.latitude === 0 && c.longitude === 0);
}

function setCandidateCoordinates(candidate, coordinates) {
  const lat = toNumberOrNull(coordinates?.latitude ?? coordinates?.lat);
  const lng = toNumberOrNull(coordinates?.longitude ?? coordinates?.lng ?? coordinates?.lon);
  if (!validLatLng(lat, lng)) return candidate;
  candidate.coordinates = { latitude: lat, longitude: lng };
  candidate.latitude = lat;
  candidate.longitude = lng;
  return candidate;
}

function buildMapsUrl(name, coordinates) {
  if (coordinates?.latitude != null && coordinates?.longitude != null) {
    return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(`${coordinates.latitude},${coordinates.longitude}`)}`;
  }
  if (name) return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(name)}`;
  return null;
}

function readScore(candidate) {
  const direct = candidate.final_confidence ?? candidate.confidence ?? candidate.score ?? candidate.rating;
  if (direct != null) return toNumberOrNull(direct);
  return toNumberOrNull(candidate.scores?.final_confidence ?? candidate.scores?.vision_landmark_score ?? null);
}

function normalizeCandidate(candidate = {}, source = 'analyzer_candidate') {
  const coordinates = normalizeCoordinates(candidate);
  const name = candidate.name || candidate.place_name || candidate.formatted_name || candidate.displayName?.text || candidate.description || candidate.title || 'Unknown place';
  const score = readScore(candidate);
  return {
    name,
    formatted_address: candidate.formatted_address || candidate.formattedAddress || candidate.address || candidate.vicinity || candidate.shortFormattedAddress || '',
    coordinates,
    latitude: coordinates.latitude,
    longitude: coordinates.longitude,
    final_confidence: score,
    confidence_label: candidate.confidence_label || candidate.confidenceLabel || null,
    reasons: candidate.reasons || candidate.match_reasons || candidate.reason || [],
    maps_url: candidate.maps_url || candidate.google_maps_url || candidate.googleMapsUri || buildMapsUrl(name, coordinates),
    place_id: candidate.place_id || candidate.id || candidate.placeId || null,
    source: candidate.source || candidate.kind || source,
    raw: candidate
  };
}

function candidateKey(candidate) {
  const lat = candidate.coordinates?.latitude ?? candidate.latitude ?? '';
  const lng = candidate.coordinates?.longitude ?? candidate.longitude ?? '';
  return [String(candidate.name || '').toLowerCase().trim(), String(candidate.formatted_address || '').toLowerCase().trim(), lat, lng].join('|');
}

function landmarkToCandidate(landmark, index = 0) {
  const coordinates = normalizeCoordinates(landmark);
  const name = landmark.description || landmark.name || 'Vision landmark';
  return normalizeCandidate({
    name,
    description: name,
    formatted_address: landmark.formatted_address || landmark.address || '',
    coordinates,
    latitude: coordinates.latitude,
    longitude: coordinates.longitude,
    score: landmark.score ?? (index === 0 ? 0.72 : 0.58),
    confidence_label: 'vision_landmark',
    reasons: ['Detected directly as a landmark by Google Vision.'],
    maps_url: buildMapsUrl(name, coordinates),
    source: 'vision_landmark'
  }, 'vision_landmark');
}

function webEntityLooksLikePlace(text) {
  const value = String(text || '').trim();
  const t = value.toLowerCase();
  if (t.length < 3) return false;
  const reject = ['photograph', 'image', 'stock photography', 'tourism', 'travel', 'vacation', 'sky', 'water', 'tree', 'wall', 'floor', 'architecture', 'rock', 'sand', 'nature', 'landscape'];
  if (reject.includes(t)) return false;
  const placeWords = ['cave', 'caves', 'temple', 'palace', 'castle', 'tower', 'bridge', 'harbour', 'harbor', 'beach', 'park', 'national', 'city', 'village', 'island', 'mountain', 'lake', 'river', 'valley', 'canyon', 'museum', 'monument', 'square', 'church', 'cathedral', 'mosque', 'ruins', 'site', 'heritage'];
  return placeWords.some(w => t.includes(w)) || /^[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ' -]+$/.test(value);
}

function entityToCandidate(entity, index = 0) {
  const name = entity.description || entity.name;
  return normalizeCandidate({
    name,
    description: name,
    formatted_address: '',
    score: Math.max(0.2, Math.min(0.64, Number(entity.score || 0.4) * 0.25 + 0.18 - index * 0.03)),
    confidence_label: 'vision_web_entity',
    reasons: ['Generated from web entities detected by Google Vision and resolved with Places when possible.'],
    maps_url: buildMapsUrl(name, {}),
    source: 'vision_web_entity'
  }, 'vision_web_entity');
}

function collectFallbackCandidates(fullJson) {
  const out = [];
  const add = c => { if (c?.name) out.push(c); };

  const landmarkSources = [
    fullJson?.visual_summary?.landmarks,
    fullJson?.landmarks,
    ...(Array.isArray(fullJson?.analyzed_images) ? fullJson.analyzed_images.map(img => img?.landmarks) : [])
  ];
  for (const source of landmarkSources) {
    if (!Array.isArray(source)) continue;
    source.forEach((landmark, i) => add(landmarkToCandidate(landmark, i)));
  }

  const entitySources = [
    fullJson?.visual_summary?.web_entities,
    fullJson?.web_entities,
    ...(Array.isArray(fullJson?.analyzed_images) ? fullJson.analyzed_images.map(img => img?.web_entities) : [])
  ];
  let entityIndex = 0;
  for (const source of entitySources) {
    if (!Array.isArray(source)) continue;
    for (const entity of source) {
      const name = entity?.description || entity?.name;
      if (!webEntityLooksLikePlace(name)) continue;
      add(entityToCandidate(entity, entityIndex++));
    }
  }

  return out;
}

function extractCandidates(fullJson, limit = 5) {
  const possibleArrays = [
    fullJson?.location_inference?.candidate_locations,
    fullJson?.location_inference?.possible_locations,
    fullJson?.candidate_locations,
    fullJson?.possible_locations,
    fullJson?.locations,
    fullJson?.results?.candidate_locations,
    fullJson?.results?.possible_locations,
    fullJson?.simple_locations,
    fullJson?.places
  ];

  const primary = possibleArrays.find(value => Array.isArray(value) && value.length > 0) || [];
  const all = [...primary.map(c => normalizeCandidate(c, 'analyzer_candidate')), ...collectFallbackCandidates(fullJson)];

  const seen = new Set();
  const unique = [];
  for (const candidate of all) {
    const key = candidateKey(candidate);
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(candidate);
  }

  unique.sort((a, b) => {
    const av = typeof a.final_confidence === 'number' ? a.final_confidence : -1;
    const bv = typeof b.final_confidence === 'number' ? b.final_confidence : -1;
    return bv - av;
  });

  return unique.slice(0, limit);
}

function mapsApiKey() {
  return process.env.GOOGLE_MAPS_API_KEY || process.env.GOOGLE_API_KEY || '';
}

function applyResolvedPlace(candidate, place, source) {
  if (!place) return false;
  const coords = normalizeCoordinates(place.location || place.geometry?.location || place.geometry || place);
  if (!validLatLng(coords.latitude, coords.longitude)) return false;

  const resolvedName = place.displayName?.text || place.name || place.formatted_address || place.formattedAddress || candidate.name;
  candidate.name = candidate.name || resolvedName;
  candidate.formatted_address = candidate.formatted_address || place.formattedAddress || place.formatted_address || place.vicinity || '';
  candidate.place_id = candidate.place_id || place.id || place.place_id || place.placeId || null;
  candidate.maps_url = place.googleMapsUri || candidate.maps_url || buildMapsUrl(resolvedName, coords);
  candidate.coordinate_source = source;
  setCandidateCoordinates(candidate, coords);
  return true;
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 7000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    const data = await response.json().catch(() => ({}));
    return { response, data };
  } finally {
    clearTimeout(timeout);
  }
}

async function resolveWithPlacesV1(candidate, query, key) {
  try {
    const { response, data } = await fetchJsonWithTimeout('https://places.googleapis.com/v1/places:searchText', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': key,
        'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.location,places.googleMapsUri'
      },
      body: JSON.stringify({ textQuery: query, maxResultCount: 1 })
    });
    if (!response.ok) return false;
    return applyResolvedPlace(candidate, data?.places?.[0], 'google_places_api_v1_text_search');
  } catch (_) {
    return false;
  }
}

async function resolveWithPlacesLegacy(candidate, query, key) {
  try {
    const url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query=' + encodeURIComponent(query) + '&key=' + encodeURIComponent(key);
    const { response, data } = await fetchJsonWithTimeout(url);
    if (!response.ok || !Array.isArray(data.results)) return false;
    return applyResolvedPlace(candidate, data.results[0], 'google_places_legacy_text_search');
  } catch (_) {
    return false;
  }
}

async function resolveWithGeocoding(candidate, query, key) {
  try {
    const url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + encodeURIComponent(query) + '&key=' + encodeURIComponent(key);
    const { response, data } = await fetchJsonWithTimeout(url);
    if (!response.ok || !Array.isArray(data.results)) return false;
    return applyResolvedPlace(candidate, data.results[0], 'google_geocoding');
  } catch (_) {
    return false;
  }
}

async function resolvePlaceForCandidate(candidate) {
  if (hasRealCoordinates(candidate)) return candidate;
  const key = mapsApiKey();
  if (!key || !candidate?.name) return candidate;

  const queries = [];
  const base = [candidate.name, candidate.formatted_address].filter(Boolean).join(' ').trim();
  if (base) queries.push(base);
  if (candidate.name && !String(candidate.name).toLowerCase().includes('barcelona') && /upc|informatika|informática|informatica|fakultatea|universidad polit[eé]cnica/i.test(candidate.name)) {
    queries.push(candidate.name + ' Barcelona Spain');
  }
  if (candidate.name && !queries.includes(candidate.name)) queries.push(candidate.name);

  for (const query of queries) {
    if (await resolveWithPlacesV1(candidate, query, key)) return candidate;
    if (await resolveWithPlacesLegacy(candidate, query, key)) return candidate;
    if (await resolveWithGeocoding(candidate, query, key)) return candidate;
  }
  return candidate;
}

async function resolveMissingCandidateCoordinates(candidates) {
  const resolved = [];
  for (const candidate of candidates) {
    resolved.push(await resolvePlaceForCandidate(candidate));
  }
  return resolved;
}

function buildSimpleLocations(candidates) {
  return candidates.map(candidate => {
    const coords = normalizeCoordinates(candidate);
    return {
      name: candidate.name,
      formatted_address: candidate.formatted_address || '',
      latitude: coords.latitude,
      longitude: coords.longitude
    };
  });
}

function updateAnalyzerJsonCandidateCoordinates(fullJson, normalizedCandidates) {
  const simple = buildSimpleLocations(normalizedCandidates);
  const updated = {
    ...fullJson,
    candidate_locations: normalizedCandidates,
    possible_locations: normalizedCandidates,
    simple_possible_locations: simple,
    frontend_candidate_locations: normalizedCandidates,
    frontend_simple_possible_locations: simple
  };

  if (!updated.location_inference || typeof updated.location_inference !== 'object') updated.location_inference = {};
  updated.location_inference.candidate_locations = normalizedCandidates.map(candidate => ({
    ...candidate.raw,
    name: candidate.name,
    formatted_address: candidate.formatted_address || candidate.raw?.formatted_address || candidate.raw?.formattedAddress || '',
    latitude: candidate.coordinates?.latitude ?? null,
    longitude: candidate.coordinates?.longitude ?? null,
    coordinates: candidate.coordinates,
    final_confidence: candidate.final_confidence,
    maps_url: candidate.maps_url,
    place_id: candidate.place_id,
    source: candidate.source
  }));
  return updated;
}

function makeProjectRelativePath(value) {
  if (typeof value !== 'string') return value;
  const normalizedRoot = ROOT_DIR.replace(/\\/g, '/');
  const normalizedValue = value.replace(/\\/g, '/');
  if (normalizedValue === normalizedRoot) return '.';
  if (normalizedValue.startsWith(normalizedRoot + '/')) {
    return './' + normalizedValue.slice(normalizedRoot.length + 1);
  }
  return value;
}

function sanitizeLocalPaths(obj, depth = 0) {
  if (depth > 20) return obj;
  if (typeof obj === 'string') return makeProjectRelativePath(obj);
  if (Array.isArray(obj)) return obj.map(item => sanitizeLocalPaths(item, depth + 1));
  if (!obj || typeof obj !== 'object') return obj;
  const cleaned = {};
  for (const [key, value] of Object.entries(obj)) cleaned[key] = sanitizeLocalPaths(value, depth + 1);
  return cleaned;
}

function enrichOutputJson(fullJson, requestInfo, candidates) {
  const updated = updateAnalyzerJsonCandidateCoordinates(fullJson, candidates);
  return sanitizeLocalPaths({ ...updated, request_info: requestInfo });
}

function buildResultForFrontend(fullJson, inputInfo, candidates) {
  const best = candidates[0] || null;
  return {
    ok: true,
    input: inputInfo,
    request_info: fullJson.request_info || null,
    best_location: best,
    possible_locations: candidates,
    simple_possible_locations: buildSimpleLocations(candidates),
    raw_summary: {
      source_input: fullJson.source_input || fullJson.source_url || fullJson.source_file || inputInfo.value,
      source_type: fullJson.source_type || inputInfo.type,
      media_type: fullJson.media_type || null,
      exact_location_found: fullJson.exact_location_found ?? fullJson?.location_inference?.exact_location_found ?? Boolean(best),
      confidence_level: fullJson.confidence_level || fullJson?.location_inference?.confidence_level || null,
      total_candidates_shown: candidates.length
    },
    full_json: fullJson
  };
}

function outputFileCandidates(exeDir) {
  return [
    OUTPUT_JSON,
    path.join(exeDir, 'output_location.json'),
    path.join(exeDir, 'output_locations.json'),
    path.join(exeDir, 'output_locations_simple.json'),
    path.join(ROOT_DIR, 'output_locations.json'),
    path.join(ROOT_DIR, 'output_locations_simple.json')
  ];
}

function findOutputJsonPath(exeDir) {
  return firstExistingPath(outputFileCandidates(exeDir));
}

function deleteOldOutputFiles(exeDir) {
  for (const file of outputFileCandidates(exeDir)) {
    try { if (fs.existsSync(file)) fs.unlinkSync(file); } catch (_) {}
  }
}

function validateConfiguration() {
  const exePath = getExecutablePath();
  return {
    executable: exePath,
    executable_exists: fs.existsSync(exePath),
    output_json_path: OUTPUT_JSON,
    output_json_exists: fs.existsSync(OUTPUT_JSON),
    has_google_api_key: Boolean(process.env.GOOGLE_API_KEY),
    has_google_vision_api_key: Boolean(process.env.GOOGLE_VISION_API_KEY),
    has_google_maps_api_key: Boolean(process.env.GOOGLE_MAPS_API_KEY),
    has_any_google_key: Boolean(process.env.GOOGLE_API_KEY || process.env.GOOGLE_VISION_API_KEY || process.env.GOOGLE_MAPS_API_KEY),
    ip_lookup_provider: 'ipapi.co',
    max_candidates: process.env.MAX_CANDIDATES || '5',
    photos_per_place: process.env.PHOTOS_PER_PLACE || '1'
  };
}

function runLocationPrototype(inputValue, inputInfo, requestInfo) {
  return new Promise((resolve, reject) => {
    const exePath = getExecutablePath();
    if (!fs.existsSync(exePath)) {
      return reject(new Error(`Could not find the C++ analyzer at: ${exePath}. Check LOCATION_EXE in .env.`));
    }

    const exeDir = path.dirname(exePath);
    deleteOldOutputFiles(exeDir);

    const maxCandidates = String(Math.max(1, Math.min(5, Number(process.env.MAX_CANDIDATES || 5))));
    const photosPerPlace = process.env.PHOTOS_PER_PLACE || '1';
    const args = [inputValue, '--max-candidates', maxCandidates, '--photos-per-place', photosPerPlace];

    const child = spawn(exePath, args, { cwd: ROOT_DIR, env: process.env, windowsHide: true });
    let stdout = '';
    let stderr = '';

    child.stdout.on('data', chunk => stdout += chunk.toString());
    child.stderr.on('data', chunk => stderr += chunk.toString());
    child.on('error', err => reject(err));

    child.on('close', async code => {
      if (code !== 0) {
        return reject(new Error(`The C++ analyzer failed with code ${code}.\n${stderr || stdout}`));
      }

      try {
        const outputPath = findOutputJsonPath(exeDir);
        if (!outputPath) {
          return reject(new Error(`The analyzer finished, but no output JSON was found in V23. I looked for output_location.json, output_locations.json and output_locations_simple.json.\nOutput:\n${stdout || stderr}`));
        }

        let fullJson = readJsonIfExists(outputPath);
        let candidates = extractCandidates(fullJson, Number(process.env.MAX_CANDIDATES || 5));
        candidates = await resolveMissingCandidateCoordinates(candidates);
        candidates = candidates.map(c => normalizeCandidate(c, c.source || 'processed_candidate'));

        candidates.sort((a, b) => {
          const av = typeof a.final_confidence === 'number' ? a.final_confidence : -1;
          const bv = typeof b.final_confidence === 'number' ? b.final_confidence : -1;
          return bv - av;
        });
        candidates = candidates.slice(0, Number(process.env.MAX_CANDIDATES || 5));

        fullJson = enrichOutputJson(fullJson, requestInfo, candidates);
        writeJson(OUTPUT_JSON, fullJson);

        resolve(buildResultForFrontend(fullJson, inputInfo, candidates));
      } catch (err) {
        reject(new Error(`Could not process the analyzer result: ${err.message}`));
      }
    });
  });
}

app.post('/api/analyze-url', async (req, res) => {
  try {
    const url = String(req.body.url || '').trim();
    if (!url) return res.status(400).json({ ok: false, error: 'Missing URL.' });
    if (!/^https?:\/\//i.test(url)) return res.status(400).json({ ok: false, error: 'The URL must start with http:// or https://.' });
    const requestInfo = await buildRequestInfo(req, 'url');
    res.json(await runLocationPrototype(url, { type: 'url', value: url }, requestInfo));
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message, config: validateConfiguration() });
  }
});

app.post('/api/analyze-file', upload.single('media'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: 'No file was received.' });
    const filePath = req.file.path;
    const relativeFilePath = path.relative(ROOT_DIR, filePath).replace(/\\/g, '/');
    const analyzerInputPath = './' + relativeFilePath;
    const requestInfo = await buildRequestInfo(req, 'file_upload', { original_filename: req.file.originalname });
    res.json(await runLocationPrototype(analyzerInputPath, { type: 'local_file', value: req.file.originalname, saved_path: analyzerInputPath }, requestInfo));
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message, config: validateConfiguration() });
  }
});

app.get('/api/health', (_req, res) => res.json({ ok: true, ...validateConfiguration() }));

app.listen(PORT, () => {
  console.log(`Interface ready at http://localhost:${PORT}`);
  console.log(`C++ analyzer: ${getExecutablePath()}`);
  console.log(`Output JSON will be written to: ${OUTPUT_JSON}`);
});
