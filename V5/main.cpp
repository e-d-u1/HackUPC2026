#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

using json = nlohmann::json;
namespace fs = std::filesystem;

struct Options {
    int maxCandidates = 5;
    int photosPerPlace = 2;
    bool keepRaw = false;
};

struct DownloadResult {
    std::string filePath;
    std::string contentType;
};

struct HttpResponse {
    long statusCode = 0;
    std::string body;
    std::string contentType;
};

static size_t writeStringCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    size_t total = size * nmemb;
    auto* str = static_cast<std::string*>(userp);
    str->append(static_cast<char*>(contents), total);
    return total;
}

static size_t writeFileCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    size_t total = size * nmemb;
    auto* file = static_cast<std::ofstream*>(userp);
    file->write(static_cast<char*>(contents), total);
    return total;
}

static size_t headerCallback(char* buffer, size_t size, size_t nitems, void* userdata) {
    size_t total = size * nitems;
    std::string header(buffer, total);
    std::string lower = header;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });

    if (lower.rfind("content-type:", 0) == 0) {
        auto* contentType = static_cast<std::string*>(userdata);
        auto pos = header.find(':');
        if (pos != std::string::npos) {
            *contentType = header.substr(pos + 1);
            while (!contentType->empty() && std::isspace(static_cast<unsigned char>(contentType->front()))) contentType->erase(contentType->begin());
            while (!contentType->empty() && std::isspace(static_cast<unsigned char>(contentType->back()))) contentType->pop_back();
        }
    }
    return total;
}

std::string getEnvAny(const std::vector<std::string>& names) {
    for (const auto& n : names) {
        const char* v = std::getenv(n.c_str());
        if (v && std::string(v).size() > 0) return std::string(v);
    }
    return "";
}

std::string shellQuote(const std::string& input) {
    // Comillas dobles: funcionan bien al ejecutar desde PowerShell/cmd en Windows
    // y permiten rutas locales con espacios.
    std::string q = "\"";
    for (char c : input) {
        if (c == '\\') q += "\\\\";
        else if (c == '"') q += "\\\"";
        else q += c;
    }
    q += "\"";
    return q;
}

std::string runCommandCapture(const std::string& command) {
    std::array<char, 256> buffer{};
    std::string result;
    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe) throw std::runtime_error("No se pudo ejecutar: " + command);
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) result += buffer.data();
    int code = pclose(pipe);
    if (code != 0) throw std::runtime_error("Comando fallido: " + command);
    return result;
}

std::string urlEncode(const std::string& value) {
    CURL* curl = curl_easy_init();
    if (!curl) return value;
    char* output = curl_easy_escape(curl, value.c_str(), static_cast<int>(value.size()));
    std::string encoded = output ? output : value;
    if (output) curl_free(output);
    curl_easy_cleanup(curl);
    return encoded;
}

std::string getExtensionFromUrl(const std::string& url) {
    std::string clean = url.substr(0, url.find('?'));
    auto dot = clean.find_last_of('.');
    if (dot == std::string::npos) return "";
    std::string ext = clean.substr(dot + 1);
    std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c) { return std::tolower(c); });
    return ext;
}
bool isUrlInput(const std::string& input) {
    std::string lower = input;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
    return lower.rfind("http://", 0) == 0 || lower.rfind("https://", 0) == 0;
}

std::string getExtensionFromPathOrUrl(const std::string& input) {
    if (isUrlInput(input)) return getExtensionFromUrl(input);
    fs::path p(input);
    std::string ext = p.extension().string();
    if (!ext.empty() && ext[0] == '.') ext.erase(ext.begin());
    std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c) { return std::tolower(c); });
    return ext;
}

std::string guessContentTypeFromExtension(const std::string& ext) {
    if (ext == "jpg" || ext == "jpeg") return "image/jpeg";
    if (ext == "png") return "image/png";
    if (ext == "webp") return "image/webp";
    if (ext == "bmp") return "image/bmp";
    if (ext == "gif") return "image/gif";
    if (ext == "mp4" || ext == "m4v") return "video/mp4";
    if (ext == "mov") return "video/quicktime";
    if (ext == "avi") return "video/x-msvideo";
    if (ext == "mkv") return "video/x-matroska";
    if (ext == "webm") return "video/webm";
    return "application/octet-stream";
}

std::string normalizeLocalPath(const std::string& input) {
    fs::path p(input);
    if (!fs::exists(p)) {
        throw std::runtime_error("No existe el archivo local: " + input);
    }
    if (fs::is_directory(p)) {
        throw std::runtime_error("La ruta local apunta a una carpeta, no a un archivo: " + input);
    }
    return fs::absolute(p).string();
}

bool isImage(const std::string& contentType, const std::string& ext) {
    if (contentType.find("image/") != std::string::npos) return true;
    return ext == "jpg" || ext == "jpeg" || ext == "png" || ext == "webp" || ext == "bmp" || ext == "gif";
}

bool isVideo(const std::string& contentType, const std::string& ext) {
    if (contentType.find("video/") != std::string::npos) return true;
    return ext == "mp4" || ext == "mov" || ext == "avi" || ext == "mkv" || ext == "webm" || ext == "m4v";
}

DownloadResult downloadFile(const std::string& url, const std::string& outputPath) {
    CURL* curl = curl_easy_init();
    if (!curl) throw std::runtime_error("No se pudo inicializar CURL");

    std::ofstream file(outputPath, std::ios::binary);
    if (!file) throw std::runtime_error("No se pudo crear archivo: " + outputPath);

    std::string contentType;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeFileCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &file);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, headerCallback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &contentType);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "hackupc-location-prototype/1.0");

    CURLcode res = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    curl_easy_cleanup(curl);
    file.close();

    if (res != CURLE_OK) throw std::runtime_error(std::string("Error descargando: ") + curl_easy_strerror(res));
    if (code < 200 || code >= 300) throw std::runtime_error("HTTP " + std::to_string(code) + " descargando archivo");
    return {outputPath, contentType};
}

HttpResponse httpPostJson(const std::string& url, const json& body, const std::vector<std::string>& headers) {
    CURL* curl = curl_easy_init();
    if (!curl) throw std::runtime_error("No se pudo inicializar CURL");

    std::string responseBody, contentType;
    std::string bodyStr = body.dump();
    struct curl_slist* headerList = nullptr;
    for (const auto& h : headers) headerList = curl_slist_append(headerList, h.c_str());

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, bodyStr.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerList);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeStringCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBody);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, headerCallback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &contentType);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    CURLcode res = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    curl_slist_free_all(headerList);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) throw std::runtime_error(std::string("Error POST: ") + curl_easy_strerror(res));
    return {code, responseBody, contentType};
}

HttpResponse httpGetToString(const std::string& url, const std::vector<std::string>& headers = {}) {
    CURL* curl = curl_easy_init();
    if (!curl) throw std::runtime_error("No se pudo inicializar CURL");

    std::string responseBody, contentType;
    struct curl_slist* headerList = nullptr;
    for (const auto& h : headers) headerList = curl_slist_append(headerList, h.c_str());

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerList);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeStringCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBody);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, headerCallback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &contentType);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    CURLcode res = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    if (headerList) curl_slist_free_all(headerList);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) throw std::runtime_error(std::string("Error GET: ") + curl_easy_strerror(res));
    return {code, responseBody, contentType};
}

void downloadUrlToFile(const std::string& url, const std::string& path, const std::vector<std::string>& headers = {}) {
    CURL* curl = curl_easy_init();
    if (!curl) throw std::runtime_error("No se pudo inicializar CURL");

    std::ofstream file(path, std::ios::binary);
    if (!file) throw std::runtime_error("No se pudo crear: " + path);

    struct curl_slist* headerList = nullptr;
    for (const auto& h : headers) headerList = curl_slist_append(headerList, h.c_str());

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerList);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeFileCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &file);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "hackupc-location-prototype/1.0");

    CURLcode res = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    if (headerList) curl_slist_free_all(headerList);
    curl_easy_cleanup(curl);
    file.close();

    if (res != CURLE_OK) throw std::runtime_error(std::string("Error descargando foto: ") + curl_easy_strerror(res));
    if (code < 200 || code >= 300) throw std::runtime_error("HTTP " + std::to_string(code) + " descargando foto");
}

std::vector<unsigned char> readBinaryFile(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) throw std::runtime_error("No se pudo abrir: " + path);
    return std::vector<unsigned char>(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
}

std::string base64Encode(const std::vector<unsigned char>& data) {
    static const char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    int val = 0, valb = -6;
    for (unsigned char c : data) {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0) {
            out.push_back(table[(val >> valb) & 0x3F]);
            valb -= 6;
        }
    }
    if (valb > -6) out.push_back(table[((val << 8) >> (valb + 8)) & 0x3F]);
    while (out.size() % 4) out.push_back('=');
    return out;
}

double getVideoDurationSeconds(const std::string& videoPath) {
    std::string cmd = "ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 " + shellQuote(videoPath);
    std::string out = runCommandCapture(cmd);
    return std::stod(out);
}

std::vector<std::string> extractThreeFrames(const std::string& videoPath, const std::string& outputDir) {
    fs::create_directories(outputDir);
    double duration = getVideoDurationSeconds(videoPath);
    if (duration <= 0) throw std::runtime_error("Duración de vídeo no válida");

    std::vector<double> stamps = {duration * 0.25, duration * 0.50, duration * 0.75};
    std::vector<std::string> frames;
    for (size_t i = 0; i < stamps.size(); ++i) {
        std::string frame = outputDir + "/frame_" + std::to_string(i + 1) + ".jpg";
        std::ostringstream cmd;
        cmd << "ffmpeg -y -ss " << stamps[i] << " -i " << shellQuote(videoPath)
            << " -frames:v 1 -q:v 2 " << shellQuote(frame) << " > NUL 2>&1";
        int code = std::system(cmd.str().c_str());
        if (code != 0) {
            // Fallback para MSYS/Linux.
            std::ostringstream cmd2;
            cmd2 << "ffmpeg -y -ss " << stamps[i] << " -i " << shellQuote(videoPath)
                 << " -frames:v 1 -q:v 2 " << shellQuote(frame) << " > /dev/null 2>&1";
            code = std::system(cmd2.str().c_str());
        }
        if (code != 0) throw std::runtime_error("No se pudo extraer frame con ffmpeg");
        frames.push_back(frame);
    }
    return frames;
}

json callVision(const std::string& imagePath) {
    std::string key = getEnvAny({"GOOGLE_VISION_API_KEY", "GOOGLE_API_KEY"});
    if (key.empty()) throw std::runtime_error("Falta GOOGLE_VISION_API_KEY o GOOGLE_API_KEY");

    std::string b64 = base64Encode(readBinaryFile(imagePath));
    json body = {
        {"requests", json::array({{
            {"image", {{"content", b64}}},
            {"features", json::array({
                {{"type", "LABEL_DETECTION"}, {"maxResults", 20}},
                {{"type", "TEXT_DETECTION"}, {"maxResults", 10}},
                {{"type", "LANDMARK_DETECTION"}, {"maxResults", 10}},
                {{"type", "LOGO_DETECTION"}, {"maxResults", 10}},
                {{"type", "WEB_DETECTION"}, {"maxResults", 10}},
                {{"type", "SAFE_SEARCH_DETECTION"}}
            })}
        }})}
    };

    std::string endpoint = "https://vision.googleapis.com/v1/images:annotate?key=" + urlEncode(key);
    HttpResponse r = httpPostJson(endpoint, body, {"Content-Type: application/json"});
    if (r.statusCode < 200 || r.statusCode >= 300) {
        throw std::runtime_error("Vision API HTTP " + std::to_string(r.statusCode) + ": " + r.body);
    }
    return json::parse(r.body);
}

std::string lowerCopy(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

bool containsAny(const std::string& text, const std::vector<std::string>& words) {
    std::string l = lowerCopy(text);
    for (const auto& w : words) if (l.find(lowerCopy(w)) != std::string::npos) return true;
    return false;
}

json simplifyVision(const json& raw, bool includeRaw, const std::string& imagePath) {
    json s;
    s["image_path"] = imagePath;
    s["labels"] = json::array();
    s["web_entities"] = json::array();
    s["landmarks"] = json::array();
    s["logos"] = json::array();
    s["text"] = "";
    s["safe_search"] = json::object();

    if (!raw.contains("responses") || raw["responses"].empty()) return s;
    const json& r = raw["responses"][0];

    if (r.contains("labelAnnotations")) {
        for (const auto& x : r["labelAnnotations"]) {
            s["labels"].push_back({{"description", x.value("description", "")}, {"score", x.value("score", 0.0)}});
        }
    }
    if (r.contains("fullTextAnnotation")) s["text"] = r["fullTextAnnotation"].value("text", "");
    else if (r.contains("textAnnotations") && !r["textAnnotations"].empty()) s["text"] = r["textAnnotations"][0].value("description", "");

    if (r.contains("landmarkAnnotations")) {
        for (const auto& x : r["landmarkAnnotations"]) {
            json item = {{"description", x.value("description", "")}, {"score", x.value("score", 0.0)}};
            if (x.contains("locations") && !x["locations"].empty()) {
                const auto& ll = x["locations"][0]["latLng"];
                item["lat"] = ll.value("latitude", 0.0);
                item["lng"] = ll.value("longitude", 0.0);
            }
            s["landmarks"].push_back(item);
        }
    }
    if (r.contains("logoAnnotations")) {
        for (const auto& x : r["logoAnnotations"]) s["logos"].push_back({{"description", x.value("description", "")}, {"score", x.value("score", 0.0)}});
    }
    if (r.contains("webDetection") && r["webDetection"].contains("webEntities")) {
        for (const auto& x : r["webDetection"]["webEntities"]) {
            std::string desc = x.value("description", "");
            if (!desc.empty()) s["web_entities"].push_back({{"description", desc}, {"score", x.value("score", 0.0)}});
        }
    }
    if (r.contains("safeSearchAnnotation")) s["safe_search"] = r["safeSearchAnnotation"];
    if (includeRaw) s["raw"] = raw;
    return s;
}

std::set<std::string> extractTerms(const json& v) {
    std::set<std::string> terms;
    auto add = [&](const std::string& term) {
        if (term.size() >= 3) terms.insert(lowerCopy(term));
    };
    if (v.contains("labels")) for (const auto& x : v["labels"]) add(x.value("description", ""));
    if (v.contains("web_entities")) for (const auto& x : v["web_entities"]) add(x.value("description", ""));
    if (v.contains("landmarks")) for (const auto& x : v["landmarks"]) add(x.value("description", ""));
    if (v.contains("logos")) for (const auto& x : v["logos"]) add(x.value("description", ""));
    return terms;
}

json mergeVisualAnalyses(const std::vector<json>& analyses) {
    std::map<std::string, double> labels, entities;
    json landmarks = json::array();
    json logos = json::array();
    std::string combinedText;

    for (const auto& a : analyses) {
        for (const auto& x : a.value("labels", json::array())) {
            std::string d = x.value("description", "");
            labels[d] = std::max(labels[d], x.value("score", 0.0));
        }
        for (const auto& x : a.value("web_entities", json::array())) {
            std::string d = x.value("description", "");
            entities[d] = std::max(entities[d], x.value("score", 0.0));
        }
        for (const auto& x : a.value("landmarks", json::array())) landmarks.push_back(x);
        for (const auto& x : a.value("logos", json::array())) logos.push_back(x);
        if (!a.value("text", "").empty()) combinedText += a.value("text", "") + "\n";
    }

    auto topMap = [](const std::map<std::string, double>& m, int maxN) {
        std::vector<std::pair<std::string, double>> v(m.begin(), m.end());
        std::sort(v.begin(), v.end(), [](auto& a, auto& b) { return a.second > b.second; });
        json arr = json::array();
        for (int i = 0; i < static_cast<int>(v.size()) && i < maxN; ++i) arr.push_back({{"description", v[i].first}, {"score", v[i].second}});
        return arr;
    };

    return {
        {"text", combinedText},
        {"labels", topMap(labels, 15)},
        {"web_entities", topMap(entities, 15)},
        {"landmarks", landmarks},
        {"logos", logos}
    };
}


bool isWeakVisualTerm(const std::string& term) {
    static const std::vector<std::string> weak = {
        "human body", "fashion", "fun", "photo shoot", "happiness", "holiday",
        "model", "foot", "calf", "barefoot", "people in nature", "travel",
        "vacation", "summer", "person", "people", "clothing", "skin", "smile"
    };
    std::string l = lowerCopy(term);
    return std::find(weak.begin(), weak.end(), l) != weak.end();
}

std::vector<std::string> topVisualTermsForFallback(const json& vision, int maxTerms) {
    std::vector<std::pair<std::string, double>> scored;

    auto add = [&](const json& arr, double weight) {
        for (const auto& x : arr) {
            std::string d = x.value("description", "");
            if (d.size() < 3 || isWeakVisualTerm(d)) continue;
            scored.push_back({d, x.value("score", 0.0) * weight});
        }
    };

    add(vision.value("web_entities", json::array()), 1.20);
    add(vision.value("labels", json::array()), 1.00);
    add(vision.value("landmarks", json::array()), 1.50);

    std::sort(scored.begin(), scored.end(), [](const auto& a, const auto& b) { return a.second > b.second; });

    std::vector<std::string> out;
    std::set<std::string> seen;
    for (const auto& kv : scored) {
        std::string key = lowerCopy(kv.first);
        if (!seen.count(key)) {
            seen.insert(key);
            out.push_back(kv.first);
        }
        if (static_cast<int>(out.size()) >= maxTerms) break;
    }
    return out;
}

void addFallbackLocationQueries(const json& vision, std::vector<std::string>& queries, std::set<std::string>& seen) {
    auto add = [&](std::string q) {
        if (q.size() < 3) return;
        std::string key = lowerCopy(q);
        if (!seen.count(key)) {
            seen.insert(key);
            queries.push_back(q);
        }
    };

    std::vector<std::string> terms = topVisualTermsForFallback(vision, 6);
    if (terms.empty()) return;

    std::ostringstream compact;
    for (size_t i = 0; i < terms.size() && i < 4; ++i) compact << terms[i] << " ";

    add(compact.str() + "tourist attraction");
    add(compact.str() + "travel destination");
    add(compact.str() + "scenic location");

    for (const auto& t : terms) {
        add(t + " tourist attraction");
        add(t + " travel destination");
    }
}

json filterDominatedCandidates(json verified, int maxCandidates) {
    json filtered = json::array();
    if (verified.empty()) return filtered;

    double top = verified[0]["scores"].value("final_confidence", 0.0);
    int removed = 0;

    for (size_t i = 0; i < verified.size(); ++i) {
        double score = verified[i]["scores"].value("final_confidence", 0.0);
        bool clearlyWorse = false;

        if (i > 0 && top >= 0.65) {
            clearlyWorse = (top - score >= 0.22) || (score <= top * 0.70);
        }

        if (!clearlyWorse && static_cast<int>(filtered.size()) < maxCandidates) {
            filtered.push_back(verified[i]);
        } else if (clearlyWorse) {
            removed++;
        }
    }

    if (filtered.empty()) filtered.push_back(verified[0]);
    filtered[0]["ranking_filter"] = {
        {"dominant_candidate_filter_applied", removed > 0},
        {"removed_clearly_worse_candidates", removed},
        {"rule", "Si el mejor candidato tiene score >= 0.65, se eliminan candidatos con diferencia >= 0.22 o score <= 70% del mejor."}
    };
    return filtered;
}

std::vector<std::string> buildLocationQueries(const json& vision) {
    std::vector<std::string> queries;
    std::set<std::string> seen;
    auto add = [&](std::string q) {
        if (q.size() < 3) return;
        if (!seen.count(lowerCopy(q))) {
            seen.insert(lowerCopy(q));
            queries.push_back(q);
        }
    };

    for (const auto& lm : vision.value("landmarks", json::array())) add(lm.value("description", ""));

    std::vector<std::string> geoWords = {"park", "national", "beach", "mountain", "desert", "dune", "lake", "river", "valley", "canyon", "island", "castle", "tower", "temple", "bridge", "palace", "square", "monument", "museum", "city", "village", "alps", "glacier", "sand"};

    for (const auto& e : vision.value("web_entities", json::array())) {
        std::string d = e.value("description", "");
        if (containsAny(d, geoWords)) {
            add(d);
            add(d + " tourist attraction");
            add(d + " location");
        }
    }

    std::vector<std::string> labelWords;
    for (const auto& l : vision.value("labels", json::array())) {
        std::string d = l.value("description", "");
        if (containsAny(d, geoWords)) labelWords.push_back(d);
    }
    if (!labelWords.empty()) {
        std::ostringstream q;
        for (size_t i = 0; i < labelWords.size() && i < 5; ++i) q << labelWords[i] << " ";
        add(q.str() + "tourist location");
    }

    std::string text = vision.value("text", "");
    if (text.size() >= 4) add(text + " location");

    if (queries.size() < 4) {
        addFallbackLocationQueries(vision, queries, seen);
    }

    if (queries.size() > 14) queries.resize(14);
    return queries;
}

json searchPlacesText(const std::string& query, int pageSize) {
    std::string key = getEnvAny({"GOOGLE_MAPS_API_KEY", "GOOGLE_API_KEY", "GOOGLE_VISION_API_KEY"});
    if (key.empty()) throw std::runtime_error("Falta GOOGLE_MAPS_API_KEY o GOOGLE_API_KEY");

    json body = {{"textQuery", query}, {"pageSize", pageSize}};
    std::vector<std::string> headers = {
        "Content-Type: application/json",
        "X-Goog-Api-Key: " + key,
        "X-Goog-FieldMask: places.id,places.displayName,places.formattedAddress,places.location,places.types,places.rating,places.photos"
    };
    HttpResponse r = httpPostJson("https://places.googleapis.com/v1/places:searchText", body, headers);
    if (r.statusCode < 200 || r.statusCode >= 300) {
        throw std::runtime_error("Places Text Search HTTP " + std::to_string(r.statusCode) + ": " + r.body);
    }
    return json::parse(r.body);
}

std::string downloadPlacePhoto(const std::string& photoName, const std::string& outputPath) {
    std::string key = getEnvAny({"GOOGLE_MAPS_API_KEY", "GOOGLE_API_KEY", "GOOGLE_VISION_API_KEY"});
    if (key.empty()) throw std::runtime_error("Falta GOOGLE_MAPS_API_KEY o GOOGLE_API_KEY");

    std::string url = "https://places.googleapis.com/v1/" + photoName + "/media?maxHeightPx=600&maxWidthPx=600&key=" + urlEncode(key);
    downloadUrlToFile(url, outputPath);
    return outputPath;
}

std::set<std::string> importantTerms(const json& vision) {
    std::set<std::string> terms;
    std::vector<std::string> weak = {"human body", "fashion", "fun", "photo shoot", "happiness", "holiday", "model", "foot", "calf", "barefoot", "people in nature", "travel", "vacation", "summer"};
    for (const auto& x : vision.value("labels", json::array())) {
        std::string d = lowerCopy(x.value("description", ""));
        if (d.size() >= 3 && std::find(weak.begin(), weak.end(), d) == weak.end()) terms.insert(d);
    }
    for (const auto& x : vision.value("web_entities", json::array())) {
        std::string d = lowerCopy(x.value("description", ""));
        if (d.size() >= 3 && std::find(weak.begin(), weak.end(), d) == weak.end()) terms.insert(d);
    }
    for (const auto& x : vision.value("landmarks", json::array())) {
        std::string d = lowerCopy(x.value("description", ""));
        if (d.size() >= 3) terms.insert(d);
    }
    return terms;
}

double jaccardScore(const std::set<std::string>& a, const std::set<std::string>& b, std::vector<std::string>& matched) {
    if (a.empty() || b.empty()) return 0.0;
    int inter = 0;
    for (const auto& x : a) {
        if (b.count(x)) {
            inter++;
            matched.push_back(x);
        }
    }
    int uni = static_cast<int>(a.size() + b.size() - inter);
    if (uni <= 0) return 0.0;
    return static_cast<double>(inter) / static_cast<double>(uni);
}

double visionEntityBoostForPlace(const json& originalVision, const json& place) {
    std::string name = lowerCopy(place.value("name", ""));
    std::string address = lowerCopy(place.value("formatted_address", ""));
    double boost = 0.0;
    for (const auto& e : originalVision.value("web_entities", json::array())) {
        std::string d = lowerCopy(e.value("description", ""));
        double score = e.value("score", 0.0);
        if (!d.empty() && (name.find(d) != std::string::npos || address.find(d) != std::string::npos)) {
            boost += std::min(0.35, score * 0.15);
        }
    }
    return std::min(0.45, boost);
}

json buildCandidateFromPlace(const json& p, const std::string& query) {
    json c;
    c["query_used"] = query;
    c["place_id"] = p.value("id", "");
    c["name"] = p.contains("displayName") ? p["displayName"].value("text", "") : "";
    c["formatted_address"] = p.value("formattedAddress", "");
    if (p.contains("location")) {
        c["latitude"] = p["location"].value("latitude", 0.0);
        c["longitude"] = p["location"].value("longitude", 0.0);
    }
    c["types"] = p.value("types", json::array());
    if (p.contains("rating") && !p["rating"].is_null()) {
        c["rating"] = p["rating"];
    } else {
        c["rating"] = nullptr;
    }
    c["photos"] = p.value("photos", json::array());
    return c;
}

std::vector<json> getPlaceCandidates(const std::vector<std::string>& queries, int maxCandidates) {
    std::map<std::string, json> byId;
    for (const auto& q : queries) {
        json res;
        try { res = searchPlacesText(q, 5); }
        catch (const std::exception& e) { std::cerr << "Aviso Places query fallida: " << q << " -> " << e.what() << "\n"; continue; }
        for (const auto& p : res.value("places", json::array())) {
            json c = buildCandidateFromPlace(p, q);
            std::string id = c.value("place_id", "");
            if (id.empty()) id = c.value("name", "") + c.value("formatted_address", "");
            if (!byId.count(id)) byId[id] = c;
        }
    }
    std::vector<json> out;
    for (auto& kv : byId) out.push_back(kv.second);
    if (static_cast<int>(out.size()) > maxCandidates * 2) out.resize(maxCandidates * 2);
    return out;
}

json verifyCandidateWithPhotos(const json& originalVision, json candidate, int photosPerPlace, bool keepRaw) {
    fs::create_directories("tmp/place_photos");

    std::set<std::string> originalTerms = importantTerms(originalVision);
    double bestVisual = 0.0;
    std::vector<std::string> bestMatches;
    json checked = json::array();

    int count = 0;
    for (const auto& photo : candidate.value("photos", json::array())) {
        if (count >= photosPerPlace) break;
        std::string photoName = photo.value("name", "");
        if (photoName.empty()) continue;

        std::string safeId = candidate.value("place_id", "place");
        std::replace(safeId.begin(), safeId.end(), '/', '_');
        std::string path = "tmp/place_photos/" + safeId + "_" + std::to_string(count + 1) + ".jpg";

        try {
            downloadPlacePhoto(photoName, path);
            json raw = callVision(path);
            json simp = simplifyVision(raw, keepRaw, path);
            std::set<std::string> photoTerms = importantTerms(simp);
            std::vector<std::string> matches;
            double sim = jaccardScore(originalTerms, photoTerms, matches);

            // Pequeño boost si una foto del sitio comparte términos geográficos importantes.
            sim = std::min(1.0, sim * 2.5);
            if (sim > bestVisual) {
                bestVisual = sim;
                bestMatches = matches;
            }

            checked.push_back({
                {"photo_path", path},
                {"visual_similarity_score", sim},
                {"matched_terms", matches}
            });
            count++;
        } catch (const std::exception& e) {
            checked.push_back({{"photo_name", photoName}, {"error", e.what()}});
        }
    }

    double entityBoost = visionEntityBoostForPlace(originalVision, candidate);
    double hasPhotosBoost = count > 0 ? 0.08 : 0.0;
    double queryScore = 0.25;
    double finalScore = std::min(1.0, queryScore + entityBoost + hasPhotosBoost + 0.45 * bestVisual);

    candidate.erase("photos");
    candidate["scores"] = {
        {"vision_entity_match", entityBoost},
        {"best_photo_visual_similarity", bestVisual},
        {"final_confidence", finalScore}
    };
    candidate["matched_visual_terms"] = bestMatches;
    candidate["photos_checked"] = checked;

    std::vector<std::string> reasons;
    if (entityBoost > 0.0) reasons.push_back("El nombre/dirección del lugar coincide con entidades detectadas por Vision.");
    if (bestVisual > 0.0) reasons.push_back("Algunas fotos del lugar comparten pistas visuales con la imagen original.");
    if (count == 0) reasons.push_back("No se pudieron comprobar fotos del lugar; confianza limitada.");
    if (reasons.empty()) reasons.push_back("Candidato generado a partir de las queries visuales, pero con evidencia débil.");
    candidate["reasons"] = reasons;

    return candidate;
}

json inferLocations(const json& originalVision, const Options& options) {
    std::vector<std::string> queries = buildLocationQueries(originalVision);
    std::vector<json> candidates = getPlaceCandidates(queries, options.maxCandidates);

    json verified = json::array();
    for (auto& c : candidates) {
        verified.push_back(verifyCandidateWithPhotos(originalVision, c, options.photosPerPlace, options.keepRaw));
    }

    std::sort(verified.begin(), verified.end(), [](const json& a, const json& b) {
        return a["scores"].value("final_confidence", 0.0) > b["scores"].value("final_confidence", 0.0);
    });
    verified = filterDominatedCandidates(verified, options.maxCandidates);

    bool exact = false;
    std::string level = "low";
    if (!verified.empty()) {
        double top = verified[0]["scores"].value("final_confidence", 0.0);
        if (top >= 0.75) level = "high";
        else if (top >= 0.50) level = "medium";
        if (top >= 0.85 && originalVision.value("landmarks", json::array()).size() > 0) exact = true;
    }

    return {
        {"exact_location_found", exact},
        {"confidence_level", level},
        {"queries_generated", queries},
        {"candidate_locations", verified}
    };
}

Options parseOptions(int argc, char* argv[]) {
    Options opt;
    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--keep-raw") opt.keepRaw = true;
        else if (a == "--max-candidates" && i + 1 < argc) opt.maxCandidates = std::stoi(argv[++i]);
        else if (a == "--photos-per-place" && i + 1 < argc) opt.photosPerPlace = std::stoi(argv[++i]);
    }
    opt.maxCandidates = std::max(1, std::min(10, opt.maxCandidates));
    opt.photosPerPlace = std::max(0, std::min(5, opt.photosPerPlace));
    return opt;
}

json processInput(const std::string& input, const Options& options) {
    fs::create_directories("tmp");

    bool inputIsUrl = isUrlInput(input);
    std::string ext = getExtensionFromPathOrUrl(input);
    std::string inputPath;
    std::string contentType;

    if (inputIsUrl) {
        inputPath = "tmp/input_file" + (ext.empty() ? std::string("") : "." + ext);
        DownloadResult dl = downloadFile(input, inputPath);
        contentType = dl.contentType.empty() ? guessContentTypeFromExtension(ext) : dl.contentType;
    } else {
        inputPath = normalizeLocalPath(input);
        contentType = guessContentTypeFromExtension(ext);
    }

    std::vector<std::string> images;
    std::string mediaType;
    if (isImage(contentType, ext)) {
        mediaType = "image";
        images.push_back(inputPath);
    } else if (isVideo(contentType, ext)) {
        mediaType = "video";
        images = extractThreeFrames(inputPath, "tmp/frames");
    } else {
        throw std::runtime_error("No se detecta imagen/vídeo. Extensión: " + ext + ", Content-Type: " + contentType);
    }

    std::vector<json> analyses;
    for (const auto& img : images) {
        json raw = callVision(img);
        analyses.push_back(simplifyVision(raw, options.keepRaw, img));
    }

    json merged = mergeVisualAnalyses(analyses);
    json locations = inferLocations(merged, options);

    json out;
    out["source_input"] = input;
    out["source_type"] = inputIsUrl ? "url" : "local_file";
    if (inputIsUrl) out["source_url"] = input;
    else out["source_file"] = inputPath;
    out["media_type"] = mediaType;
    out["content_type"] = contentType;
    out["analyzed_images"] = analyses;
    out["visual_summary"] = merged;
    out["location_inference"] = locations;
    out["note"] = "final_confidence es un score heurístico, no una probabilidad matemática exacta. En vídeos o imágenes sin pistas claras, los candidatos pueden ser lugares visualmente parecidos, no ubicaciones confirmadas.";
    return out;
}

int main(int argc, char* argv[]) {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    try {
        if (argc < 2) {
            std::cerr << "Uso:\n  location_prototype.exe <URL_O_RUTA_LOCAL_IMAGEN_O_VIDEO> [--max-candidates N] [--photos-per-place N] [--keep-raw]\n\nEjemplos:\n  location_prototype.exe \"https://example.com/foto.jpg\"\n  location_prototype.exe \"..\\media\\foto.jpg\"\n  location_prototype.exe \"..\\media\\video.mp4\"\n";
            curl_global_cleanup();
            return 1;
        }
        std::string input = argv[1];
        Options options = parseOptions(argc, argv);
        json result = processInput(input, options);

        std::ofstream out("output_location.json");
        out << result.dump(2);
        out.close();

        std::cout << "Análisis completado. Resultado creado:\n";
        std::cout << "  - output_location.json\n";
        curl_global_cleanup();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        curl_global_cleanup();
        return 1;
    }
}
