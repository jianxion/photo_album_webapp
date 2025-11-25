const API_CONFIG = {
    // API Gateway base URL
    API_BASE_URL: 'https://2jgplrfxsj.execute-api.us-east-1.amazonaws.com/dev',
    
    // Endpoints
    SEARCH_ENDPOINT: '/search',
    UPLOAD_ENDPOINT: '/upload',
    
    API_KEY: 'RSMw2fvaRU8QayMj443WH5gYVUy5rXCl9qBa7mA9'
};

if (typeof module !== 'undefined' && module.exports) {
    module.exports = API_CONFIG;
}
