import pandas as pd
import kagglehub
import os

# Descargar dataset
path = kagglehub.dataset_download("juanmah/world-cities")

# Buscar el CSV dentro de la carpeta
files = os.listdir(path)
csv_file = [f for f in files if f.endswith(".csv")][0]
csv_path = os.path.join(path, csv_file)

# Cargar dataset
df = pd.read_csv(csv_path)

# ⚠️ IMPORTANTE: nombres reales de columnas en este dataset
# normalmente: city, country, iso2, population, lat, lng

df = df.rename(columns={
    'city': 'city_name',
    'country': 'country_name',
    'iso2': 'country_code'
})

# Asegurar población numérica
df['population'] = pd.to_numeric(df['population'], errors='coerce')

# 1. Filtrar ciudades grandes
df = df[df['population'] > 100000]

# 2. Quitar duplicados
df = df.drop_duplicates(subset=['city_name', 'country_code'])

# 3. Añadir continente (manual mapping)
continent_map = {
    'AF': 'Africa', 'AL': 'Europe', 'DZ': 'Africa', 'AR': 'South America',
    'AU': 'Oceania', 'NZ': 'Oceania', 'PG': 'Oceania', 'AT': 'Europe', 'BD': 'Asia', 'BE': 'Europe',
    'BR': 'South America', 'CA': 'North America', 'CN': 'Asia',
    'CO': 'South America', 'EG': 'Africa', 'FR': 'Europe',
    'DE': 'Europe', 'IN': 'Asia', 'ID': 'Asia', 'IT': 'Europe',
    'JP': 'Asia', 'MX': 'North America', 'NG': 'Africa',
    'RU': 'Europe', 'SA': 'Asia', 'ZA': 'Africa',
    'KR': 'Asia', 'ES': 'Europe', 'SE': 'Europe',
    'CH': 'Europe', 'TR': 'Asia', 'GB': 'Europe',
    'US': 'North America'
}

df['continent'] = df['country_code'].map(continent_map)

# ⚠️ eliminar los que no tienen continente asignado
df = df.dropna(subset=['continent'])

# 4. Ordenar por población
df = df.sort_values(by='population', ascending=False)

# 5. Distribución
distribution = {
    'Asia': 200,
    'Europe': 200,
    'Africa': 200,
    'North America': 150,
    'South America': 200,
    'Oceania': 50
}

# 6. Selección
selected = []

for continent, n in distribution.items():
    subset = df[df['continent'] == continent].head(n)
    print(f"{continent}: {len(subset)} ciudades")
    selected.append(subset)

result = pd.concat(selected)

# 7. Guardar
result.to_csv("filtered_cities.csv", index=False)

print(f"\nTotal ciudades seleccionadas: {len(result)}")