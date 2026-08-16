const EARTH_R = 6371

export function haversineKm(lat1, lon1, lat2, lon2) {
  const dLat = ((lat2 - lat1) * Math.PI) / 180
  const dLon = ((lon2 - lon1) * Math.PI) / 180
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) * Math.cos((lat2 * Math.PI) / 180) * Math.sin(dLon / 2) ** 2
  return EARTH_R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

export function formatDistanceKm(km) {
  if (km < 1) return `${Math.round(km * 1000)}m`
  return `${km.toFixed(1)}km`
}

// Nearest instance of each type; ties broken by lower load, then capacity headroom.
export function nearestOfEachType(services, lat, lng) {
  const byType = {}
  for (const s of services) {
    if (s.status !== 'ONLINE') continue
    if (!byType[s.type]) byType[s.type] = []
    byType[s.type].push({ ...s, distanceKm: haversineKm(lat, lng, s.lat, s.lng) })
  }
  const result = {}
  for (const type of Object.keys(byType)) {
    const list = byType[type].sort((a, b) => {
      const d = a.distanceKm - b.distanceKm
      if (Math.abs(d) > 0.5) return d
      const loadA = a.currentLoad / Math.max(1, a.capacity)
      const loadB = b.currentLoad / Math.max(1, b.capacity)
      if (loadA !== loadB) return loadA - loadB
      return (b.capacity - b.currentLoad) - (a.capacity - a.currentLoad)
    })
    result[type] = list[0]
  }
  return result
}