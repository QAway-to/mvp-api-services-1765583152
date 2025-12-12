// Bitrix24 Webhook endpoint - receives events from Bitrix and syncs to Shopify
// NOTE: Admin API functionality is DISABLED - using webhooks only
// import { callShopifyAdmin, getOrder, updateOrder } from '../../../src/lib/shopify/adminClient.js';
import { callBitrix } from '../../../src/lib/bitrix/client.js';

// Configure body parser
export const config = {
  api: {
    bodyParser: {
      sizeLimit: '1mb',
    },
  },
};

/**
 * Handle deal update event from Bitrix
 * NOTE: Admin API functionality is DISABLED - this handler is disabled
 * Bitrix → Shopify sync is not implemented (using webhooks only)
 */
async function handleDealUpdate(deal) {
  console.log(`[BITRIX WEBHOOK] ⚠️ handleDealUpdate is disabled - admin API functionality is turned off`);
  console.log(`[BITRIX WEBHOOK] Deal update received for deal ${deal.ID || deal.id}, but Shopify sync is disabled`);
  // Admin API functionality is disabled - no Shopify sync from Bitrix
  // All synchronization is done via Shopify webhooks → Bitrix
  return;
  
  /* DISABLED - Admin API functionality
  const shopifyOrderId = deal.UF_SHOPIFY_ORDER_ID || deal.uf_shopify_order_id;
  if (!shopifyOrderId) {
    console.log(`[BITRIX WEBHOOK] Deal ${deal.ID || deal.id} has no UF_SHOPIFY_ORDER_ID, skipping Shopify sync`);
    return;
  }

  // Get current order from Shopify using admin API
  const shopifyOrder = await getOrder(shopifyOrderId);
  // ... rest of the sync logic ...
  */
}

/**
 * Handle deal creation event from Bitrix
 * Usually not needed as deals are created from Shopify, but handle for completeness
 */
async function handleDealCreate(deal) {
  console.log(`[BITRIX WEBHOOK] Handling deal create: ${deal.ID || deal.id}`);
  // Deals are typically created from Shopify, so this is usually a no-op
  // But we can log it for monitoring
}

/**
 * Main webhook handler
 */
export default async function handler(req, res) {
  console.log(`[BITRIX WEBHOOK] ===== INCOMING REQUEST =====`);
  console.log(`[BITRIX WEBHOOK] Method: ${req.method}`);
  console.log(`[BITRIX WEBHOOK] Headers:`, {
    'content-type': req.headers['content-type'],
    'user-agent': req.headers['user-agent']
  });
  
  if (req.method !== 'POST') {
    console.log(`[BITRIX WEBHOOK] ❌ Method not allowed: ${req.method}`);
    res.status(405).end('Method not allowed');
    return;
  }

  const event = req.body;
  const eventType = event.event || event.EVENT || 'unknown';

  console.log(`[BITRIX WEBHOOK] Event type: ${eventType}`);
  console.log(`[BITRIX WEBHOOK] Event data:`, JSON.stringify(event, null, 2));

  try {
    // Bitrix webhook format: { event: 'ONCRMDEALUPDATE', data: { FIELDS: {...} } }
    // Or direct format: { ID: ..., STAGE_ID: ..., ... }
    
    let deal = null;
    if (event.data && event.data.FIELDS) {
      deal = event.data.FIELDS;
    } else if (event.FIELDS) {
      deal = event.FIELDS;
    } else {
      deal = event; // Direct deal object
    }

    if (!deal || (!deal.ID && !deal.id)) {
      console.error(`[BITRIX WEBHOOK] Invalid event format: no deal ID found`);
      res.status(400).json({ error: 'Invalid event format' });
      return;
    }

    // Route based on event type
    if (eventType === 'ONCRMDEALUPDATE' || eventType.includes('UPDATE')) {
      await handleDealUpdate(deal);
    } else if (eventType === 'ONCRMDEALADD' || eventType.includes('ADD')) {
      await handleDealCreate(deal);
    } else {
      console.log(`[BITRIX WEBHOOK] Unhandled event type: ${eventType}`);
    }

    res.status(200).json({ success: true, message: 'Event processed' });
  } catch (e) {
    console.error('[BITRIX WEBHOOK] Error:', e);
    res.status(500).json({ error: 'Internal server error', message: e.message });
  }
}

