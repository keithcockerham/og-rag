// O&G RAG Assistant - Frontend JavaScript
// Calls Azure Functions API for RAG queries

// API Configuration - Update this after deploying Azure Function
const API_BASE = 'https://og-rag-api.azurewebsites.net/api';  // Update with your Azure Function URL

// DOM Elements
const queryInput = document.getElementById('queryInput');
const submitBtn = document.getElementById('submitQuery');
const btnText = document.getElementById('btnText');
const btnSpinner = document.getElementById('btnSpinner');
const topKSlider = document.getElementById('topK');
const topKValue = document.getElementById('topKValue');
const minScoreSlider = document.getElementById('minScore');
const minScoreValue = document.getElementById('minScoreValue');
const resultsSection = document.getElementById('resultsSection');
const errorSection = document.getElementById('errorSection');
const answerContent = document.getElementById('answerContent');
const sourcesList = document.getElementById('sourcesList');
const sourceCount = document.getElementById('sourceCount');
const errorMessage = document.getElementById('errorMessage');

// Update slider displays
topKSlider.addEventListener('input', () => {
    topKValue.textContent = topKSlider.value;
});

minScoreSlider.addEventListener('input', () => {
    minScoreValue.textContent = parseFloat(minScoreSlider.value).toFixed(2);
});

// Sample query buttons
document.querySelectorAll('.sample-tag').forEach(btn => {
    btn.addEventListener('click', () => {
        queryInput.value = btn.dataset.query;
        queryInput.focus();
    });
});

// Submit on Enter (but allow Shift+Enter for newlines)
queryInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        submitQuery();
    }
});

// Submit button click
submitBtn.addEventListener('click', submitQuery);

// Main query function
async function submitQuery() {
    const query = queryInput.value.trim();
    
    if (!query) {
        showError('Please enter a question');
        return;
    }
    
    // Show loading state
    setLoading(true);
    hideResults();
    hideError();
    
    try {
        const response = await fetch(`${API_BASE}/query`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                query: query,
                top_k: parseInt(topKSlider.value),
                min_score: parseFloat(minScoreSlider.value)
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.error || `API returned ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.error) {
            throw new Error(data.error);
        }
        
        displayResults(data);
        
    } catch (error) {
        console.error('Query error:', error);
        showError(error.message || 'Failed to get response from API');
    } finally {
        setLoading(false);
    }
}

// Display results
function displayResults(data) {
    // Display answer
    answerContent.innerHTML = formatAnswer(data.answer);
    
    // Display sources
    sourcesList.innerHTML = '';
    sourceCount.textContent = `(${data.sources.length} retrieved)`;
    
    data.sources.forEach((source, index) => {
        const sourceCard = createSourceCard(source, index);
        sourcesList.appendChild(sourceCard);
    });
    
    // Show results section
    resultsSection.classList.remove('hidden');
    
    // Scroll to results
    resultsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// Format answer with basic markdown-like formatting
function formatAnswer(text) {
    // Convert markdown-style formatting
    let html = text
        // Headers
        .replace(/^### (.+)$/gm, '<h4>$1</h4>')
        .replace(/^## (.+)$/gm, '<h3>$1</h3>')
        // Bold
        .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
        // Italic
        .replace(/\*(.+?)\*/g, '<em>$1</em>')
        // Line breaks to paragraphs
        .split('\n\n')
        .map(para => para.trim())
        .filter(para => para)
        .map(para => {
            // Check if it's a list
            if (para.match(/^[-•*]\s/m)) {
                const items = para.split('\n')
                    .map(line => line.replace(/^[-•*]\s/, '').trim())
                    .filter(line => line)
                    .map(line => `<li>${line}</li>`)
                    .join('');
                return `<ul>${items}</ul>`;
            }
            // Check if it's a numbered list
            if (para.match(/^\d+\.\s/m)) {
                const items = para.split('\n')
                    .map(line => line.replace(/^\d+\.\s/, '').trim())
                    .filter(line => line)
                    .map(line => `<li>${line}</li>`)
                    .join('');
                return `<ol>${items}</ol>`;
            }
            return `<p>${para.replace(/\n/g, '<br>')}</p>`;
        })
        .join('');
    
    return html;
}

// Create source card element
function createSourceCard(source, index) {
    const card = document.createElement('div');
    card.className = 'source-card';
    
    const scoreColor = source.score >= 0.8 ? '#38ef7d' : 
                       source.score >= 0.7 ? '#11998e' : '#718096';
    
    card.innerHTML = `
        <div class="source-header" onclick="toggleSource(${index})">
            <div class="source-meta">
                <span class="source-badge">${source.source}</span>
                <span class="source-type">${source.doc_type}</span>
            </div>
            <div style="display: flex; align-items: center; gap: 1rem;">
                <span class="source-score" style="color: ${scoreColor}">
                    ${(source.score * 100).toFixed(1)}% match
                </span>
                <span class="expand-icon" id="icon-${index}">▼</span>
            </div>
        </div>
        <div class="source-content" id="content-${index}">
            <p>${escapeHtml(source.text)}</p>
            <p class="source-file">📄 ${source.source_file}</p>
        </div>
    `;
    
    return card;
}

// Toggle source content visibility
function toggleSource(index) {
    const content = document.getElementById(`content-${index}`);
    const icon = document.getElementById(`icon-${index}`);
    
    content.classList.toggle('expanded');
    icon.classList.toggle('rotated');
}

// Escape HTML to prevent XSS
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// UI State functions
function setLoading(loading) {
    submitBtn.disabled = loading;
    btnText.textContent = loading ? 'Searching...' : 'Search & Generate';
    btnSpinner.classList.toggle('hidden', !loading);
}

function hideResults() {
    resultsSection.classList.add('hidden');
}

function hideError() {
    errorSection.classList.add('hidden');
}

function showError(message) {
    errorMessage.textContent = message;
    errorSection.classList.remove('hidden');
    resultsSection.classList.add('hidden');
}

// Make toggleSource available globally
window.toggleSource = toggleSource;

// Export for debugging
window.ragApi = {
    API_BASE,
    submitQuery
};
