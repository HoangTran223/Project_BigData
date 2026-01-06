// Air Quality Dashboard JavaScript

const API_BASE = window.location.origin;

// DOM Elements
const countrySelect = document.getElementById('country-select');
const stationSelect = document.getElementById('station-select');
const stationSelectorSection = document.getElementById('station-selector-section');
const refreshBtn = document.getElementById('refresh-btn');
const loading = document.getElementById('loading');
const error = document.getElementById('error');
const content = document.getElementById('content');

// State
let currentCountry = null;
let currentStationId = null;
let stationsList = [];

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    countrySelect.addEventListener('change', handleCountryChange);
    stationSelect.addEventListener('change', handleStationChange);
    refreshBtn.addEventListener('click', handleRefresh);
});

// Handle country selection
async function handleCountryChange() {
    const countryCode = countrySelect.value;
    if (countryCode) {
        currentCountry = countryCode;
        await loadStations(countryCode);
    } else {
        hideStationSelector();
        hideContent();
    }
}

// Handle station selection
function handleStationChange() {
    const stationId = stationSelect.value;
    if (stationId) {
        currentStationId = stationId;
        loadStationData(stationId);
    } else {
        hideContent();
    }
}

// Handle refresh button
function handleRefresh() {
    if (currentStationId) {
        loadStationData(currentStationId);
    } else if (currentCountry) {
        loadStations(currentCountry);
    }
}

// Load stations for selected country
async function loadStations(countryCode) {
    showLoading();
    hideError();
    hideContent();
    hideStationSelector();
    
    try {
        const response = await fetch(`${API_BASE}/api/stations/${countryCode}`);
        const data = await response.json();
        
        if (!data.success) {
            showError(data.error || 'Không thể tải danh sách trạm đo');
            return;
        }
        
        stationsList = data.stations;
        
        // Populate station selector
        stationSelect.innerHTML = '<option value="">-- Chọn trạm đo --</option>';
        stationsList.forEach(station => {
            const option = document.createElement('option');
            option.value = station.location_id;
            option.textContent = station.location_name;
            stationSelect.appendChild(option);
        });
        
        showStationSelector();
        hideLoading();
        
    } catch (err) {
        console.error('Error loading stations:', err);
        showError('Lỗi kết nối đến server. Vui lòng thử lại sau.');
        hideLoading();
    }
}

// Load data for selected station
async function loadStationData(stationId) {
    showLoading();
    hideError();
    hideContent();
    
    try {
        const response = await fetch(`${API_BASE}/api/station/${stationId}`);
        const data = await response.json();
        
        if (!data.success) {
            showError(data.error || 'Không thể tải dữ liệu trạm đo');
            return;
        }
        
        displayStationData(data);
        hideLoading();
        showContent();
    } catch (err) {
        console.error('Error loading station data:', err);
        showError('Lỗi kết nối đến server. Vui lòng thử lại sau.');
        hideLoading();
    }
}

// Display station data on the page
function displayStationData(data) {
    const station = data.station;
    
    // Update station info
    document.getElementById('station-name').textContent = station.location_name;
    document.getElementById('station-location').textContent = station.country;
    document.getElementById('station-coordinates').innerHTML = `
        <div style="color: var(--gray-600); font-size: 0.875rem; margin-top: 0.5rem;">
            📍 ${station.latitude.toFixed(4)}, ${station.longitude.toFixed(4)}
        </div>
    `;
    
    // Update overall AQI
    const overallAqi = data.overall.aqi;
    const overallCategory = data.overall.category;
    const recommendation = data.overall.recommendation;
    
    document.getElementById('overall-aqi').textContent = overallAqi;
    document.getElementById('overall-aqi').className = `aqi-value aqi-${getAqiColorClass(overallAqi)}`;
    document.getElementById('overall-category').textContent = overallCategory;
    
    // Update recommendation
    document.getElementById('recommendation-icon').textContent = recommendation.icon;
    document.getElementById('recommendation-text').textContent = recommendation.message;
    document.getElementById('recommendation').className = `recommendation recommendation-${recommendation.color}`;
    
    // Display parameters
    displayParameters(data.parameters);
    
    // Display prediction if available
    if (data.prediction) {
        displayPrediction(data.prediction);
    } else {
        hidePrediction();
    }
}

// Display prediction
function displayPrediction(prediction) {
    const predictionCard = document.getElementById('prediction-card');
    const predictionAqi = document.getElementById('prediction-aqi');
    const predictionCategory = document.getElementById('prediction-category');
    const predictionIcon = document.getElementById('prediction-icon');
    const predictionText = document.getElementById('prediction-text');
    const predictionRecommendation = document.getElementById('prediction-recommendation');
    
    if (!predictionCard || !prediction) return;
    
    const aqi = Math.round(prediction.aqi_next_hour);
    const category = prediction.category;
    const rec = prediction.recommendation;
    const aqiColorClass = getAqiColorClass(aqi);
    
    predictionAqi.textContent = aqi;
    predictionAqi.className = `prediction-value aqi-${aqiColorClass}`;
    predictionCategory.textContent = category;
    predictionCategory.className = `prediction-category aqi-${aqiColorClass}`;
    
    predictionIcon.textContent = rec.icon;
    predictionText.textContent = rec.message;
    predictionRecommendation.className = `prediction-recommendation recommendation-${rec.color}`;
    
    predictionCard.classList.remove('hidden');
}

// Hide prediction
function hidePrediction() {
    const predictionCard = document.getElementById('prediction-card');
    if (predictionCard) {
        predictionCard.classList.add('hidden');
    }
}

// Display parameters
function displayParameters(parameters) {
    const grid = document.getElementById('parameters-grid');
    grid.innerHTML = '';
    
    const paramNames = {
        'pm25': 'PM2.5',
        'pm10': 'PM10',
        'o3': 'O₃',
        'co': 'CO',
        'so2': 'SO₂',
        'no2': 'NO₂'
    };
    
    for (const [param, data] of Object.entries(parameters)) {
        if (!data) continue;
        
        const card = document.createElement('div');
        card.className = 'parameter-card';
        
        const aqiColorClass = getAqiColorClass(data.aqi);
        
        card.innerHTML = `
            <div class="parameter-header">
                <span class="parameter-name">${paramNames[param] || param.toUpperCase()}</span>
                <span class="parameter-category aqi-${aqiColorClass}">${data.category}</span>
            </div>
            <div class="parameter-value aqi-${aqiColorClass}">${data.value}</div>
            <div class="parameter-details">
                <div class="parameter-detail-item">
                    <span class="parameter-detail-label">Giá trị:</span>
                    <span class="parameter-detail-value">${data.value} ${data.unit}</span>
                </div>
                <div class="parameter-detail-item">
                    <span class="parameter-detail-label">AQI:</span>
                    <span class="parameter-detail-value aqi-${aqiColorClass}">${data.aqi}</span>
                </div>
            </div>
        `;
        
        grid.appendChild(card);
    }
}


// Get AQI color class
function getAqiColorClass(aqi) {
    if (aqi <= 50) return 'good';
    if (aqi <= 100) return 'moderate';
    if (aqi <= 150) return 'unhealthy-sensitive';
    if (aqi <= 200) return 'unhealthy';
    if (aqi <= 300) return 'very-unhealthy';
    return 'hazardous';
}

// UI Helper Functions
function showLoading() {
    loading.classList.remove('hidden');
}

function hideLoading() {
    loading.classList.add('hidden');
}

function showError(message) {
    error.textContent = message;
    error.classList.remove('hidden');
}

function hideError() {
    error.classList.add('hidden');
}

function showContent() {
    content.classList.remove('hidden');
}

function hideContent() {
    content.classList.add('hidden');
}

function showStationSelector() {
    stationSelectorSection.classList.remove('hidden');
}

function hideStationSelector() {
    stationSelectorSection.classList.add('hidden');
    stationSelect.innerHTML = '<option value="">-- Chọn trạm đo --</option>';
}

