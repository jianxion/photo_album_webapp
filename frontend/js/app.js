let selectedFile = null;

// Search functionality
async function searchPhotos() {
    const query = document.getElementById('searchInput').value.trim();
    
    if (!query) {
        showStatus('searchStatus', 'Please enter a search query', 'error');
        return;
    }

    showStatus('searchStatus', 'Searching...', 'info');
    document.getElementById('resultsGrid').innerHTML = '';

    try {
        const response = await fetch(
            `${API_CONFIG.API_BASE_URL}${API_CONFIG.SEARCH_ENDPOINT}?q=${encodeURIComponent(query)}`,
            {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': API_CONFIG.API_KEY
                }
            }
        );

        if (!response.ok) {
            throw new Error(`Search failed: ${response.status} ${response.statusText}`);
        }

        const data = await response.json();
        displayResults(data);
        
        const resultCount = data.results ? data.results.length : 0;
        showStatus('searchStatus', `Found ${resultCount} photo(s)`, 'success');
    } catch (error) {
        console.error('Search error:', error);
        showStatus('searchStatus', `Error: ${error.message}`, 'error');
    }
}


function displayResults(data) {
    const resultsGrid = document.getElementById('resultsGrid');
    const resultsTitle = document.getElementById('resultsTitle');
    
    if (!data.results || data.results.length === 0) {
        resultsGrid.innerHTML = '<div class="no-results">No photos found. Try a different search term.</div>';
        resultsTitle.textContent = 'No Results';
        return;
    }

    resultsTitle.textContent = `Search Results (${data.results.length})`;
    
    resultsGrid.innerHTML = data.results.map(photo => `
        <div class="photo-card">
            <img src="${photo.url}" alt="Photo" onerror="this.src='data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 width=%22300%22 height=%22200%22%3E%3Crect fill=%22%23ddd%22 width=%22300%22 height=%22200%22/%3E%3Ctext x=%2250%25%22 y=%2250%25%22 text-anchor=%22middle%22 dy=%22.3em%22 fill=%22%23999%22%3EImage not available%3C/text%3E%3C/svg%3E'">
            <div class="photo-labels">
                ${photo.labels && photo.labels.length > 0 
                    ? photo.labels.map(label => `<span class="label">${label}</span>`).join('')
                    : '<span class="label">No labels</span>'
                }
            </div>
        </div>
    `).join('');
}

// File selection handler
function handleFileSelect(event) {
    const file = event.target.files[0];
    if (file) {
        if (!file.type.match('image.*')) {
            showStatus('uploadStatus', 'Please select an image file', 'error');
            return;
        }
        
        selectedFile = file;
        document.getElementById('fileName').textContent = file.name;
        document.getElementById('uploadBtn').disabled = false;
        showStatus('uploadStatus', `Selected: ${file.name}`, 'info');
    }
}

// Upload photo
async function uploadPhoto() {
    if (!selectedFile) {
        showStatus('uploadStatus', 'Please select a photo first', 'error');
        return;
    }

    const customLabels = document.getElementById('customLabels').value.trim();
    const uploadBtn = document.getElementById('uploadBtn');
    
    uploadBtn.disabled = true;
    showStatus('uploadStatus', 'Uploading...', 'info');

    try {
        const fileName = selectedFile.name;
        const url = `${API_CONFIG.API_BASE_URL}${API_CONFIG.UPLOAD_ENDPOINT}?key=${encodeURIComponent(fileName)}`;
        
        const headers = {
            'Content-Type': selectedFile.type,
            'x-api-key': API_CONFIG.API_KEY
        };
        
        // Add custom labels if provided
        if (customLabels) {
            headers['x-amz-meta-customLabels'] = customLabels;
        }

        const response = await fetch(url, {
            method: 'PUT',
            headers: headers,
            body: selectedFile
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Upload failed: ${response.status} - ${errorText}`);
        }

        showStatus('uploadStatus', '✅ Photo uploaded successfully!', 'success');
        
        // Reset form
        selectedFile = null;
        document.getElementById('fileInput').value = '';
        document.getElementById('fileName').textContent = '';
        document.getElementById('customLabels').value = '';
        uploadBtn.disabled = true;
        
        // Clear status after 3 seconds
        setTimeout(() => {
            document.getElementById('uploadStatus').textContent = '';
        }, 3000);
        
    } catch (error) {
        console.error('Upload error:', error);
        showStatus('uploadStatus', `Error: ${error.message}`, 'error');
        uploadBtn.disabled = false;
    }
}


function showStatus(elementId, message, type) {
    const element = document.getElementById(elementId);
    element.textContent = message;
    element.className = `status-message ${type}`;
}


document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('searchInput').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            searchPhotos();
        }
    });
});
