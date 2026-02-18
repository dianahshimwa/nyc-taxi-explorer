const API = 'http://localhost:5000/api'

document.addEventListener('DOMContentLoaded', () => {
    loadStats()
    loadHourlyChart()
    loadTopZones()
    loadDistanceChart()
    loadBoroughChart()
    loadTrips()

    document.getElementById('apply-filters')
        .addEventListener('click', loadTrips)

    document.getElementById('reset-filters')
        .addEventListener('click', resetFilters)
})


async function loadStats() {
    const data = await fetchData(`${API}/stats`)
    if (!data) return

    document.getElementById('total-trips').textContent =
        data.total_trips.toLocaleString()

    document.getElementById('avg-fare').textContent =
        `$${data.average_fare.toFixed(2)}`

    document.getElementById('total-revenue').textContent =
        `$${(data.total_revenue / 1000000).toFixed(2)}M`

    document.getElementById('rush-hour').textContent =
        `${data.rush_hour_pct}%`

    document.getElementById('avg-distance').textContent =
        `${data.average_distance} mi`
}

async function loadHourlyChart() {
    const data = await fetchData(`${API}/hourly`)
    if (!data) return

    const ctx = document.getElementById('hourly-chart').getContext('2d')

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => `${d.hour}:00`),
            datasets: [{
                label: 'Trips',
                data: data.map(d => d.trip_count),
                borderColor: '#e94560',
                backgroundColor: 'rgba(233, 69, 96, 0.1)',
                tension: 0.4,
                fill: true,
                pointRadius: 3
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    })
}

async function loadTopZones() {
    const data = await fetchData(`${API}/top-zones?limit=10`)
    if (!data) return

    const ctx = document.getElementById('zones-chart').getContext('2d')

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.zone),
            datasets: [{
                label: 'Revenue ($)',
                data: data.map(d => d.revenue),
                backgroundColor: '#1a1a2e'
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            plugins: { legend: { display: false } }
        }
    })
}

async function loadDistanceChart() {
    const data = await fetchData(`${API}/distance-distribution`)
    if (!data) return

    const ctx = document.getElementById('distance-chart').getContext('2d')

    new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: data.map(d => d.range),
            datasets: [{
                data: data.map(d => d.count),
                backgroundColor: [
                    '#e94560',
                    '#1a1a2e',
                    '#16213e',
                    '#0f3460',
                    '#533483'
                ]
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'bottom' }
            }
        }
    })
}

async function loadBoroughChart() {
    const data = await fetchData(`${API}/boroughs`)
    if (!data) return

    const ctx = document.getElementById('borough-chart').getContext('2d')

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.borough),
            datasets: [{
                label: 'Trips',
                data: data.map(d => d.trip_count),
                backgroundColor: '#e94560'
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true } }
        }
    })
}

async function loadTrips() {
    const borough  = document.getElementById('borough-filter').value
    const minFare  = document.getElementById('min-fare').value
    const maxFare  = document.getElementById('max-fare').value
    const rushHour = document.getElementById('rush-filter').value
    const sortBy   = document.getElementById('sort-by').value
    const limit    = document.getElementById('limit-select').value

    let url = `${API}/trips?limit=${limit}&min_fare=${minFare}&max_fare=${maxFare}&sort_by=${sortBy}`

    if (borough)  url += `&borough=${borough}`
    if (rushHour !== '') url += `&rush_hour=${rushHour}`

    const trips = await fetchData(url)
    if (!trips) return

    const tbody = document.getElementById('trips-body')
    tbody.innerHTML = ''

    if (trips.length === 0) {
        tbody.innerHTML = '<tr><td colspan="9">No trips match your filters</td></tr>'
        return
    }

    trips.forEach(trip => {
        const row = document.createElement('tr')

        const time = new Date(trip.pickup_datetime)
            .toLocaleString('en-US', {
                month:  'short',
                day:    'numeric',
                hour:   '2-digit',
                minute: '2-digit'
            })

        row.innerHTML = `
            <td>${time}</td>
            <td>${trip.pickup_zone || trip.pickup_borough || '-'}</td>
            <td>${trip.dropoff_zone || trip.dropoff_borough || '-'}</td>
            <td>${trip.trip_distance} mi</td>
            <td>${trip.duration_minutes.toFixed(0)} min</td>
            <td>${trip.speed_mph ? trip.speed_mph.toFixed(1) : '-'} mph</td>
            <td>$${trip.total_amount.toFixed(2)}</td>
            <td>${trip.payment_label || '-'}</td>
            <td class="${trip.is_rush_hour ? 'rush-yes' : 'rush-no'}">
                ${trip.is_rush_hour ? 'Yes' : 'No'}
            </td>
        `
        tbody.appendChild(row)
    })
}

function resetFilters() {
    document.getElementById('borough-filter').value = ''
    document.getElementById('min-fare').value = '0'
    document.getElementById('max-fare').value = '200'
    document.getElementById('rush-filter').value = ''
    document.getElementById('sort-by').value = 'pickup_datetime'
    document.getElementById('limit-select').value = '100'
    loadTrips()
}

async function fetchData(url) {
    try {
        const res = await fetch(url)
        if (!res.ok) throw new Error(`HTTP error: ${res.status}`)
        return await res.json()
    } catch (err) {
        console.error('Fetch error:', err.message)
        return null
    }
}