# HACKUPC Location Finder V23

V23 integrates the web interface and the C++ image/video analyzer in one folder.

## Main changes

- The generated analyzer JSON is saved directly as `V23/output_location.json`.
- User request info is saved in the JSON: `user_ip`, `user_country`, `user_country_code`, and `user_ip_location`.
- IP-to-country lookup uses `ipapi.co`. Local/private IPs such as `127.0.0.1` or `::1` are saved as `skipped` because they cannot reveal the real country.
- Candidate coordinates are normalized from `latitude/longitude`, `lat/lng`, nested `location`, nested `geometry.location`, arrays, and analyzer fields.
- If a candidate has no coordinates, the backend tries to resolve it using Google Places Text Search.

## Setup

```powershell
cd V23
Copy-Item .env.example .env
npm install
npm start
```

Open `http://localhost:3000`.

Check configuration at `http://localhost:3000/api/health`.

## Required APIs

Enable Cloud Vision API and Places API in Google Cloud.

Use either:

```txt
GOOGLE_VISION_API_KEY=YOUR_VISION_KEY
GOOGLE_MAPS_API_KEY=YOUR_MAPS_PLACES_KEY
```

or one shared key:

```txt
GOOGLE_API_KEY=YOUR_KEY
```
