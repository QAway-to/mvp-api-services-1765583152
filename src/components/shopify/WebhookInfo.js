import { useState, useEffect } from 'react';

export default function WebhookInfo({ onBitrixUrlChange }) {
  // Dynamic base URL - uses current deployment domain (no hardcoding)
  // Get base URL immediately, not in useEffect to avoid empty state
  const getBaseUrl = () => {
    if (typeof window !== 'undefined' && window.location) {
      return window.location.origin;
    }
    return '';
  };
  
  const [baseUrl] = useState(getBaseUrl);
  
  // Static webhook endpoints (paths only - URL will be built dynamically)
  const WEBHOOK_ENDPOINTS = {
    'order/crt': {
      label: 'Order Creation (order/crt)',
      path: '/api/webhook/order/crt',
      description: 'Webhook for order creation events'
    },
    'order/upd': {
      label: 'Order Update (order/upd)',
      path: '/api/webhook/order/upd',
      description: 'Webhook for order update events (includes refunds and cancellations)'
    },
    'product/upd': {
      label: 'Product Update (product/upd)',
      path: '/api/webhook/product/upd',
      description: 'Webhook for product update events (catalog only, does not affect deals)'
    }
  };

  // Static Bitrix endpoints (paths only)
  const BITRIX_ENDPOINTS = {
    'bitrix': {
      label: 'Bitrix Webhook Endpoint',
      path: '/api/webhook/bitrix',
      description: 'Endpoint to receive events from Bitrix24'
    }
  };

  const [selectedEndpoint, setSelectedEndpoint] = useState('order/crt');
  const [password, setPassword] = useState('');
  const [unlocked, setUnlocked] = useState(false);
  const [copied, setCopied] = useState(false);
  const [bitrixWebhookUrl, setBitrixWebhookUrl] = useState('https://bfcshoes.bitrix24.eu/rest/52/i6l05o71ywxb8j1l');
  const [bitrixPassword, setBitrixPassword] = useState('');
  const [bitrixUnlocked, setBitrixUnlocked] = useState(false);
  const [bitrixCopied, setBitrixCopied] = useState(false);
  const [bitrixEndpointPassword, setBitrixEndpointPassword] = useState('');
  const [bitrixEndpointUnlocked, setBitrixEndpointUnlocked] = useState(false);
  const [bitrixEndpointCopied, setBitrixEndpointCopied] = useState(false);
  
  const CORRECT_PASSWORD = '1spotify2';

  useEffect(() => {
    // Notify parent component about Bitrix URL change
    if (onBitrixUrlChange) {
      onBitrixUrlChange(bitrixWebhookUrl);
    }
  }, [bitrixWebhookUrl, onBitrixUrlChange]);

  const handleCopy = () => {
    const currentOrigin = typeof window !== 'undefined' && window.location ? window.location.origin : baseUrl;
    const selectedPath = WEBHOOK_ENDPOINTS[selectedEndpoint]?.path || '';
    const selectedUrl = currentOrigin ? `${currentOrigin}${selectedPath}` : '';
    if (selectedUrl) {
      navigator.clipboard.writeText(selectedUrl);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const handlePasswordSubmit = (e) => {
    e.preventDefault();
    if (password === CORRECT_PASSWORD) {
      setUnlocked(true);
      setPassword('');
    } else {
      alert('Incorrect password');
      setPassword('');
    }
  };

  const handleBitrixPasswordSubmit = (e) => {
    e.preventDefault();
    if (bitrixPassword === CORRECT_PASSWORD) {
      setBitrixUnlocked(true);
      setBitrixPassword('');
    } else {
      alert('Incorrect password');
      setBitrixPassword('');
    }
  };

  const handleBitrixEndpointPasswordSubmit = (e) => {
    e.preventDefault();
    if (bitrixEndpointPassword === CORRECT_PASSWORD) {
      setBitrixEndpointUnlocked(true);
      setBitrixEndpointPassword('');
    } else {
      alert('Incorrect password');
      setBitrixEndpointPassword('');
    }
  };

  const handleBitrixCopy = () => {
    navigator.clipboard.writeText(bitrixWebhookUrl);
    setBitrixCopied(true);
    setTimeout(() => setBitrixCopied(false), 2000);
  };

  const handleBitrixEndpointCopy = () => {
    const currentOrigin = typeof window !== 'undefined' && window.location ? window.location.origin : baseUrl;
    const bitrixPath = BITRIX_ENDPOINTS.bitrix.path;
    const bitrixUrl = currentOrigin ? `${currentOrigin}${bitrixPath}` : '';
    if (bitrixUrl) {
      navigator.clipboard.writeText(bitrixUrl);
      setBitrixEndpointCopied(true);
      setTimeout(() => setBitrixEndpointCopied(false), 2000);
    }
  };

  // Build full URL from base URL and path
  // Always use current origin to ensure it matches the actual deployment
  const currentOrigin = typeof window !== 'undefined' && window.location ? window.location.origin : baseUrl;
  const selectedPath = WEBHOOK_ENDPOINTS[selectedEndpoint]?.path || '';
  const selectedUrl = currentOrigin ? `${currentOrigin}${selectedPath}` : '';
  const bitrixEndpointUrl = currentOrigin ? `${currentOrigin}${BITRIX_ENDPOINTS.bitrix.path}` : '';

  return (
    <div className="card">
      <header className="card-header">
        <h2>Webhook Configuration</h2>
      </header>
      <div style={{ padding: '20px' }}>
        {/* Shopify Webhooks - Single field with dropdown */}
        <div style={{ marginBottom: '24px' }}>
          <p style={{ color: '#94a3b8', marginBottom: '12px', fontSize: '0.9rem' }}>
            Shopify webhook endpoints (URLs match current deployment - configure once in Shopify):
          </p>
          {!unlocked ? (
            <form onSubmit={handlePasswordSubmit} style={{ marginBottom: '12px' }}>
              <div style={{
                display: 'flex',
                gap: '8px',
                alignItems: 'center',
                padding: '12px',
                background: '#1e293b',
                borderRadius: '8px',
                border: '1px solid #334155'
              }}>
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Enter password to view URLs"
                  style={{
                    flex: 1,
                    background: 'transparent',
                    border: 'none',
                    color: '#f1f5f9',
                    fontSize: '0.9rem',
                    outline: 'none',
                    padding: '4px 8px'
                  }}
                />
                <button
                  type="submit"
                  className="btn"
                  style={{ whiteSpace: 'nowrap' }}
                >
                  Unlock
                </button>
              </div>
            </form>
          ) : (
            <div>
              <div style={{
                display: 'flex',
                gap: '8px',
                alignItems: 'center',
                marginBottom: '12px',
                padding: '12px',
                background: '#1e293b',
                borderRadius: '8px',
                border: '1px solid #334155'
              }}>
                <select
                  value={selectedEndpoint}
                  onChange={(e) => setSelectedEndpoint(e.target.value)}
                  style={{
                    flex: 1,
                    background: '#0f172a',
                    border: '1px solid #334155',
                    color: '#f1f5f9',
                    fontSize: '0.9rem',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    outline: 'none',
                    cursor: 'pointer'
                  }}
                >
                  {Object.entries(WEBHOOK_ENDPOINTS).map(([key, endpoint]) => (
                    <option key={key} value={key}>
                      {endpoint.label}
                    </option>
                  ))}
                </select>
                <button
                  onClick={handleCopy}
                  className="btn"
                  style={{ whiteSpace: 'nowrap' }}
                  title="Copy webhook URL"
                >
                  {copied ? '✓ Copied' : 'Copy'}
                </button>
                <button
                  onClick={() => setUnlocked(false)}
                  className="btn"
                  style={{ whiteSpace: 'nowrap', background: '#6b7280' }}
                  title="Lock URLs"
                >
                  🔒 Lock
                </button>
              </div>
              <div style={{
                padding: '12px',
                background: '#1e293b',
                borderRadius: '8px',
                border: '1px solid #334155'
              }}>
                <p style={{ color: '#94a3b8', fontSize: '0.85rem', marginBottom: '8px' }}>
                  {WEBHOOK_ENDPOINTS[selectedEndpoint].description}
                </p>
                <code style={{
                  display: 'block',
                  color: '#60a5fa',
                  fontSize: '0.85rem',
                  wordBreak: 'break-all',
                  fontFamily: 'monospace',
                  padding: '8px',
                  background: '#0f172a',
                  borderRadius: '4px'
                }}>
                  {selectedUrl}
                </code>
              </div>
            </div>
          )}
        </div>

        {/* Bitrix Webhook Endpoint */}
        <div style={{ marginBottom: '24px' }}>
          <p style={{ color: '#94a3b8', marginBottom: '12px', fontSize: '0.9rem' }}>
            Bitrix24 webhook endpoint (URL matches current deployment - configure once in Bitrix24):
          </p>
          {!bitrixEndpointUnlocked ? (
            <form onSubmit={handleBitrixEndpointPasswordSubmit} style={{ marginBottom: '12px' }}>
              <div style={{
                display: 'flex',
                gap: '8px',
                alignItems: 'center',
                padding: '12px',
                background: '#1e293b',
                borderRadius: '8px',
                border: '1px solid #334155'
              }}>
                <input
                  type="password"
                  value={bitrixEndpointPassword}
                  onChange={(e) => setBitrixEndpointPassword(e.target.value)}
                  placeholder="Enter password to view URL"
                  style={{
                    flex: 1,
                    background: 'transparent',
                    border: 'none',
                    color: '#f1f5f9',
                    fontSize: '0.9rem',
                    outline: 'none',
                    padding: '4px 8px'
                  }}
                />
                <button
                  type="submit"
                  className="btn"
                  style={{ whiteSpace: 'nowrap' }}
                >
                  Unlock
                </button>
              </div>
            </form>
          ) : (
            <div style={{
              display: 'flex',
              gap: '8px',
              alignItems: 'center',
              padding: '12px',
              background: '#1e293b',
              borderRadius: '8px',
              border: '1px solid #334155'
            }}>
              <code style={{
                flex: 1,
                color: '#60a5fa',
                fontSize: '0.9rem',
                wordBreak: 'break-all',
                fontFamily: 'monospace'
              }}>
                {bitrixEndpointUrl || 'Loading...'}
              </code>
              <button
                onClick={handleBitrixEndpointCopy}
                className="btn"
                style={{ whiteSpace: 'nowrap' }}
                title="Copy webhook URL"
              >
                {bitrixEndpointCopied ? '✓ Copied' : 'Copy'}
              </button>
              <button
                onClick={() => setBitrixEndpointUnlocked(false)}
                className="btn"
                style={{ whiteSpace: 'nowrap', background: '#6b7280' }}
                title="Lock URL"
              >
                🔒 Lock
              </button>
            </div>
          )}
        </div>

        {/* Bitrix Webhook (for sending to Bitrix) */}
        <div style={{ marginBottom: '24px' }}>
          <p style={{ color: '#94a3b8', marginBottom: '12px', fontSize: '0.9rem' }}>
            Bitrix24 webhook URL (for sending events to Bitrix24):
          </p>
          {!bitrixUnlocked ? (
            <form onSubmit={handleBitrixPasswordSubmit} style={{ marginBottom: '12px' }}>
              <div style={{
                display: 'flex',
                gap: '8px',
                alignItems: 'center',
                padding: '12px',
                background: '#1e293b',
                borderRadius: '8px',
                border: '1px solid #334155'
              }}>
                <input
                  type="password"
                  value={bitrixPassword}
                  onChange={(e) => setBitrixPassword(e.target.value)}
                  placeholder="Enter password to view/edit URL"
                  style={{
                    flex: 1,
                    background: 'transparent',
                    border: 'none',
                    color: '#f1f5f9',
                    fontSize: '0.9rem',
                    outline: 'none',
                    padding: '4px 8px'
                  }}
                />
                <button
                  type="submit"
                  className="btn"
                  style={{ whiteSpace: 'nowrap' }}
                >
                  Unlock
                </button>
              </div>
            </form>
          ) : (
            <div style={{
              display: 'flex',
              gap: '8px',
              alignItems: 'center',
              padding: '12px',
              background: '#1e293b',
              borderRadius: '8px',
              border: '1px solid #334155'
            }}>
              <input
                type="text"
                value={bitrixWebhookUrl}
                onChange={(e) => setBitrixWebhookUrl(e.target.value)}
                style={{
                  flex: 1,
                  background: 'transparent',
                  border: 'none',
                  color: '#f1f5f9',
                  fontSize: '0.9rem',
                  fontFamily: 'monospace',
                  outline: 'none',
                  padding: '4px 8px'
                }}
                placeholder="Enter Bitrix webhook URL"
              />
              <button
                onClick={handleBitrixCopy}
                className="btn"
                style={{ whiteSpace: 'nowrap' }}
                title="Copy webhook URL"
              >
                {bitrixCopied ? '✓ Copied' : 'Copy'}
              </button>
              <button
                onClick={() => setBitrixUnlocked(false)}
                className="btn"
                style={{ whiteSpace: 'nowrap', background: '#6b7280' }}
                title="Lock URL"
              >
                🔒 Lock
              </button>
            </div>
          )}
        </div>

        <div className="alert alert-info" style={{ marginTop: '20px' }}>
          <strong>Setup Instructions:</strong>
          <ol style={{ marginTop: '12px', paddingLeft: '20px', color: '#cbd5e1' }}>
            <li style={{ marginBottom: '8px' }}>Go to your Shopify Admin</li>
            <li style={{ marginBottom: '8px' }}>Navigate to Settings → Notifications → Webhooks</li>
            <li style={{ marginBottom: '8px' }}>Create 3 webhooks with these static URLs:</li>
            <ul style={{ marginLeft: '20px', marginTop: '4px', marginBottom: '8px' }}>
              <li><strong>Order creation</strong> → <code style={{fontSize: '0.85rem'}}>/api/webhook/order/crt</code></li>
              <li><strong>Order update</strong> → <code style={{fontSize: '0.85rem'}}>/api/webhook/order/upd</code> (handles refunds and cancellations)</li>
              <li><strong>Product update</strong> → <code style={{fontSize: '0.85rem'}}>/api/webhook/product/upd</code> (catalog only)</li>
            </ul>
            <li style={{ marginBottom: '8px' }}>Select format: JSON for all webhooks</li>
            <li>Save all webhooks - URLs are static and won't change</li>
          </ol>
        </div>
      </div>
    </div>
  );
}
