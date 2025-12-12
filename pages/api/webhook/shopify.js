// Shopify Webhook endpoint
import { shopifyAdapter } from '../../../src/lib/adapters/shopify/index.js';
import { callBitrix, getBitrixWebhookBase } from '../../../src/lib/bitrix/client.js';
import { mapShopifyOrderToBitrixDeal } from '../../../src/lib/bitrix/orderMapper.js';
import { upsertBitrixContact } from '../../../src/lib/bitrix/contact.js';
import { BITRIX_CONFIG, financialStatusToStageId, financialStatusToPaymentStatus } from '../../../src/lib/bitrix/config.js';

// Configure body parser to accept raw JSON
// Increased size limit for large orders with many line items
export const config = {
  api: {
    bodyParser: {
      sizeLimit: '5mb', // Increased from 1mb for large orders
    },
  },
};

/**
 * Handle order created event - create deal in Bitrix
 */
async function handleOrderCreated(order, requestId) {
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Handling order created: ${order.name || order.id}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Order data:`, {
    id: order.id,
    name: order.name,
    total_price: order.total_price,
    current_total_price: order.current_total_price,
    financial_status: order.financial_status,
    line_items_count: order.line_items?.length || 0
  });

  const shopifyOrderId = String(order.id);

  // CRITICAL: Check if deal already exists before creating
  // This prevents duplicate deals when Shopify sends multiple webhooks (orders/create + orders/updated)
  const existingDealCheck = await callBitrix('/crm.deal.list.json', {
    filter: { 
      'UF_SHOPIFY_ORDER_ID': shopifyOrderId
    },
    select: ['ID', 'UF_SHOPIFY_ORDER_ID'],
    limit: 1
  });

  const existingDeals = existingDealCheck.result || [];
  
  // Strict filter: check exact string match (Bitrix may return partial matches)
  const exactMatch = existingDeals.find(d => String(d.UF_SHOPIFY_ORDER_ID) === shopifyOrderId);
  
  if (exactMatch) {
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] ⚠️ Deal already exists for order ${shopifyOrderId} (ID: ${exactMatch.ID}). Skipping creation.`);
    return exactMatch.ID; // Return existing deal ID
  }

  console.log(`[SHOPIFY WEBHOOK] [${requestId}] No existing deal found for order ${shopifyOrderId}. Creating new deal...`);

  // Map order to Bitrix deal
  const { dealFields, productRows } = mapShopifyOrderToBitrixDeal(order);
  
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Mapped dealFields:`, JSON.stringify(dealFields, null, 2));
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Mapped productRows count:`, productRows.length);
  if (productRows.length > 0) {
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] First product row:`, JSON.stringify(productRows[0], null, 2));
  }

  // Ensure UF_SHOPIFY_ORDER_ID is set
  dealFields.UF_SHOPIFY_ORDER_ID = shopifyOrderId;

  // Upsert contact (non-blocking)
  let contactId = null;
  try {
    const bitrixBase = getBitrixWebhookBase();
    contactId = await upsertBitrixContact(bitrixBase, order);
    if (contactId) {
      dealFields.CONTACT_ID = contactId;
    }
  } catch (contactError) {
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] Contact upsert failed (non-blocking):`, contactError);
  }

  // 1. Create deal
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Sending deal to Bitrix with fields:`, Object.keys(dealFields));
  const dealAddResp = await callBitrix('/crm.deal.add.json', {
    fields: dealFields,
  });

  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Bitrix response:`, JSON.stringify(dealAddResp, null, 2));

  if (!dealAddResp.result) {
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] ❌ Failed to create deal. Response:`, dealAddResp);
    throw new Error(`Failed to create deal: ${JSON.stringify(dealAddResp)}`);
  }

  const dealId = dealAddResp.result;
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] ✅ Deal created: ${dealId}`);

  // 2. Set product rows
  if (productRows.length > 0) {
    try {
      await callBitrix('/crm.deal.productrows.set.json', {
        id: dealId,
        rows: productRows,
      });
      console.log(`[SHOPIFY WEBHOOK] [${requestId}] Product rows set for deal ${dealId}: ${productRows.length} rows`);
    } catch (productRowsError) {
      console.error(`[SHOPIFY WEBHOOK] [${requestId}] Product rows error (non-blocking):`, productRowsError);
      // Don't throw - deal is already created
    }
  }

  return dealId;
}

/**
 * Handle order updated event - update deal in Bitrix
 * This is the MAIN TRIGGER for:
 * - Updating product rows in deal
 * - Recalculating totals/discounts/taxes
 * - Updating payment status and stage
 * - Handling partial/full refunds
 */
async function handleOrderUpdated(order, requestId) {
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Handling order updated: ${order.name || order.id}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Order data:`, {
    id: order.id,
    name: order.name,
    financial_status: order.financial_status,
    total_price: order.total_price,
    current_total_price: order.current_total_price,
    line_items_count: order.line_items?.length || 0
  });

  const shopifyOrderId = String(order.id);
  const shopifyOrderIdNum = Number(order.id); // Also try as number for comparison

  // 1. Find existing deal by UF_SHOPIFY_ORDER_ID
  // Try multiple search strategies to handle Bitrix API quirks
  let allDeals = [];
  let deals = [];
  
  // Strategy 1: Filter by UF_SHOPIFY_ORDER_ID as string
  try {
  const listResp = await callBitrix('/crm.deal.list.json', {
      filter: { 
        'UF_SHOPIFY_ORDER_ID': shopifyOrderId,
        // 'CATEGORY_ID': 2  // Uncomment if needed to filter only Stock category
      },
      select: ['ID', 'OPPORTUNITY', 'STAGE_ID', 'CATEGORY_ID', 'DATE_CREATE', 'UF_SHOPIFY_ORDER_ID'],
      order: { 'DATE_CREATE': 'DESC' }, // Sort by creation date descending
    });
    allDeals = listResp.result || [];
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] Bitrix API returned ${allDeals.length} deal(s) for filter UF_SHOPIFY_ORDER_ID="${shopifyOrderId}"`);
  } catch (filterError) {
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] Error filtering by UF_SHOPIFY_ORDER_ID:`, filterError);
  }
  
  // CRITICAL: Strict filtering - Bitrix API may return partial matches or wrong format
  // Filter to only deals where UF_SHOPIFY_ORDER_ID exactly matches (as string or number)
  deals = allDeals.filter(d => {
    const dealOrderId = d.UF_SHOPIFY_ORDER_ID;
    if (!dealOrderId) return false;
    
    // Try exact string match
    if (String(dealOrderId) === shopifyOrderId) return true;
    
    // Try numeric match (handle cases where Bitrix stores as number)
    const dealOrderIdNum = Number(dealOrderId);
    if (!isNaN(dealOrderIdNum) && dealOrderIdNum === shopifyOrderIdNum) return true;
    
    // Try trimmed string match (handle whitespace issues)
    if (String(dealOrderId).trim() === shopifyOrderId.trim()) return true;
    
    return false;
  });
  
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Initial search found ${deals.length} exact match deal(s) for order ${shopifyOrderId} (filtered from ${allDeals.length} total)`);
  
  // If no exact match found, try alternative search: get recent deals and filter client-side
  if (deals.length === 0) {
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] No exact match found with filter. Trying alternative search...`);
    
    try {
      // Get recent deals (last 100) without filter and search client-side
      const recentDealsResp = await callBitrix('/crm.deal.list.json', {
        select: ['ID', 'OPPORTUNITY', 'STAGE_ID', 'CATEGORY_ID', 'DATE_CREATE', 'UF_SHOPIFY_ORDER_ID'],
        order: { 'DATE_CREATE': 'DESC' },
        limit: 100, // Check last 100 deals
      });
      
      const recentDeals = recentDealsResp.result || [];
      console.log(`[SHOPIFY WEBHOOK] [${requestId}] Checking ${recentDeals.length} recent deals for order ${shopifyOrderId}...`);
      
      // Filter client-side with strict matching
      const foundDeals = recentDeals.filter(d => {
        const dealOrderId = d.UF_SHOPIFY_ORDER_ID;
        if (!dealOrderId) return false;
        
        // Exact string match
        if (String(dealOrderId) === shopifyOrderId) return true;
        
        // Numeric match
        const dealOrderIdNum = Number(dealOrderId);
        if (!isNaN(dealOrderIdNum) && dealOrderIdNum === shopifyOrderIdNum) return true;
        
        // Trimmed match
        if (String(dealOrderId).trim() === shopifyOrderId.trim()) return true;
        
        return false;
      });
      
      if (foundDeals.length > 0) {
        deals = foundDeals;
        console.log(`[SHOPIFY WEBHOOK] [${requestId}] ✅ Found ${foundDeals.length} deal(s) using alternative search method!`);
        
        // Log the UF_SHOPIFY_ORDER_ID values found for debugging
        foundDeals.forEach((d, idx) => {
          console.log(`[SHOPIFY WEBHOOK] [${requestId}] Deal ${idx + 1}: ID=${d.ID}, UF_SHOPIFY_ORDER_ID="${d.UF_SHOPIFY_ORDER_ID}" (type: ${typeof d.UF_SHOPIFY_ORDER_ID})`);
        });
      } else {
        console.log(`[SHOPIFY WEBHOOK] [${requestId}] No deals found even with alternative search. Order ${shopifyOrderId} may be new.`);
      }
    } catch (altSearchError) {
      console.error(`[SHOPIFY WEBHOOK] [${requestId}] Alternative search failed:`, altSearchError);
    }
  }
  
  // Final count after all search attempts
  const finalDealsCount = deals.length;
  
  if (allDeals.length > finalDealsCount && finalDealsCount > 0) {
    console.warn(`[SHOPIFY WEBHOOK] [${requestId}] ⚠️ Bitrix returned ${allDeals.length} deals, but only ${finalDealsCount} have exact UF_SHOPIFY_ORDER_ID match. Using exact matches only.`);
  }

  let dealId = null;
  let deal = null;

  if (finalDealsCount === 0) {
    // No deal found - CREATE NEW DEAL
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] ⚠️ No deal found for order ${shopifyOrderId}. Creating new deal...`);
    
    // Determine category based on order tags
    const orderTags = Array.isArray(order.tags) 
      ? order.tags 
      : (order.tags ? String(order.tags).split(',').map(t => t.trim()) : []);
    
    const preorderTags = ['pre-order', 'preorder-product-added'];
    const hasPreorderTag = orderTags.some(tag => 
      preorderTags.some(preorderTag => tag.toLowerCase() === preorderTag.toLowerCase())
    );
    
    const categoryId = hasPreorderTag ? BITRIX_CONFIG.CATEGORY_PREORDER : BITRIX_CONFIG.CATEGORY_STOCK;

    // Map order to Bitrix deal
    const { dealFields, productRows } = mapShopifyOrderToBitrixDeal(order);
    
    // Ensure CATEGORY_ID and UF_SHOPIFY_ORDER_ID are set
    dealFields.CATEGORY_ID = categoryId;
    dealFields.UF_SHOPIFY_ORDER_ID = shopifyOrderId;

    // Upsert contact (non-blocking)
    let contactId = null;
    try {
      const bitrixBase = getBitrixWebhookBase();
      contactId = await upsertBitrixContact(bitrixBase, order);
      if (contactId) {
        dealFields.CONTACT_ID = contactId;
      }
    } catch (contactError) {
      console.error(`[SHOPIFY WEBHOOK] [${requestId}] Contact upsert failed (non-blocking):`, contactError);
    }

    // Create new deal
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] Creating new deal with CATEGORY_ID=${categoryId}, UF_SHOPIFY_ORDER_ID=${shopifyOrderId}`);
    const dealAddResp = await callBitrix('/crm.deal.add.json', {
      fields: dealFields,
    });

    if (!dealAddResp.result) {
      console.error(`[SHOPIFY WEBHOOK] [${requestId}] ❌ Failed to create deal. Response:`, dealAddResp);
      throw new Error(`Failed to create deal: ${JSON.stringify(dealAddResp)}`);
    }

    dealId = dealAddResp.result;
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] ✅ New deal created: ${dealId}`);

    // Set product rows for new deal
    if (productRows && productRows.length > 0) {
      await callBitrix('/crm.deal.productrows.set.json', {
        id: dealId,
        rows: productRows,
      });
      console.log(`[SHOPIFY WEBHOOK] [${requestId}] Product rows set for new deal ${dealId}: ${productRows.length} rows`);
    }

    return dealId;

  } else if (finalDealsCount === 1) {
    // Exactly one deal found - UPDATE IT
    deal = deals[0];
    dealId = deal.ID;
    const currentCategoryId = Number(deal.CATEGORY_ID) || 2;
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] ✅ Found exactly one deal ${dealId} for order ${shopifyOrderId}, category: ${currentCategoryId}`);
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] Verified UF_SHOPIFY_ORDER_ID: "${deal.UF_SHOPIFY_ORDER_ID}" === "${shopifyOrderId}"`);

  } else {
    // Multiple deals found - ERROR, but update the most recent one
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] ⚠️ DATA ERROR: Found ${finalDealsCount} deals for order ${shopifyOrderId}!`);
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] Deal IDs:`, deals.map(d => `${d.ID} (UF_SHOPIFY_ORDER_ID: "${d.UF_SHOPIFY_ORDER_ID}")`).join(', '));
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] Will update the most recent deal (sorted by DATE_CREATE DESC)`);
    
    // Use the most recent deal (already sorted by DATE_CREATE DESC)
    deal = deals[0];
    dealId = deal.ID;
    const currentCategoryId = Number(deal.CATEGORY_ID) || 2;
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] Updating deal ${dealId} (most recent), category: ${currentCategoryId}`);
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] Verified UF_SHOPIFY_ORDER_ID: "${deal.UF_SHOPIFY_ORDER_ID}" === "${shopifyOrderId}"`);
  }

  // 2. UPDATE EXISTING DEAL
  const currentCategoryId = Number(deal.CATEGORY_ID) || 2;

  // Determine category based on order tags (pre-order tags → cat_8, otherwise cat_2)
  const orderTags = Array.isArray(order.tags) 
    ? order.tags 
    : (order.tags ? String(order.tags).split(',').map(t => t.trim()) : []);
  
  const preorderTags = ['pre-order', 'preorder-product-added'];
  const hasPreorderTag = orderTags.some(tag => 
    preorderTags.some(preorderTag => tag.toLowerCase() === preorderTag.toLowerCase())
  );
  
  const categoryId = hasPreorderTag ? BITRIX_CONFIG.CATEGORY_PREORDER : BITRIX_CONFIG.CATEGORY_STOCK;

  // Prepare update fields - ALWAYS UPDATE
  const { dealFields: mappedFields } = mapShopifyOrderToBitrixDeal(order);
  
  const fields = {
    // Always update amount, discounts, taxes, shipping
    OPPORTUNITY: mappedFields.OPPORTUNITY,
    UF_SHOPIFY_TOTAL_DISCOUNT: mappedFields.UF_SHOPIFY_TOTAL_DISCOUNT || 0,
    UF_SHOPIFY_TOTAL_TAX: mappedFields.UF_SHOPIFY_TOTAL_TAX || 0,
    UF_SHOPIFY_SHIPPING_PRICE: mappedFields.UF_SHOPIFY_SHIPPING_PRICE || 0,
  };

  // Update category if changed
  if (categoryId !== currentCategoryId) {
    fields.CATEGORY_ID = categoryId;
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] Category changed from ${currentCategoryId} to ${categoryId}`);
  }

  // Map financial status to stage ID (based on category)
  // For partially_refunded: keep current stage (don't move to LOSE)
  // For cancelled/voided/refunded: ALWAYS move to LOSE
  const currentStageId = deal.STAGE_ID;
  const financialStatusLower = order.financial_status?.toLowerCase() || '';
  const stageId = financialStatusToStageId(order.financial_status, categoryId, currentStageId);
  
  // Only keep current stage for partial refunds - all other statuses update normally
  if (financialStatusLower === 'partially_refunded') {
    // Keep current stage for partial refunds - don't change to LOSE
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] Partial refund detected - keeping current stage: "${currentStageId}"`);
    // Don't update STAGE_ID field - keep it as is
  } else {
    // For other statuses (including cancelled, voided, refunded), update stage normally
    fields.STAGE_ID = stageId;
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] Financial status "${order.financial_status}" → Stage "${stageId}" (category ${categoryId}, current: "${currentStageId}")`);
    if (stageId !== currentStageId) {
      console.log(`[SHOPIFY WEBHOOK] [${requestId}] ✅ Stage updated: "${currentStageId}" → "${stageId}"`);
    } else {
      console.log(`[SHOPIFY WEBHOOK] [${requestId}] ℹ️ Stage unchanged: "${currentStageId}"`);
    }
  }

  // Payment status synchronization - ALWAYS UPDATE
  const paymentStatusEnumId = financialStatusToPaymentStatus(order.financial_status);
  fields.UF_CRM_1739183959976 = paymentStatusEnumId;
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Payment status: "${paymentStatusEnumId}" (financial_status: ${order.financial_status})`);

  // Update order type and delivery method if present
  if (mappedFields.UF_CRM_1739183268662) {
    fields.UF_CRM_1739183268662 = mappedFields.UF_CRM_1739183268662; // Order type
  }
  if (mappedFields.UF_CRM_1739183302609) {
    fields.UF_CRM_1739183302609 = mappedFields.UF_CRM_1739183302609; // Delivery method
  }

  // 3. Update deal fields
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Updating deal ${dealId} with fields:`, Object.keys(fields));
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Deal ${dealId} UF_SHOPIFY_ORDER_ID: "${deal.UF_SHOPIFY_ORDER_ID}" (verifying match with order ${shopifyOrderId})`);
  
  await callBitrix('/crm.deal.update.json', {
    id: dealId,
    fields,
  });
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] ✅ Deal ${dealId} updated successfully`);

  // 4. ALWAYS UPDATE PRODUCT ROWS - full replacement from line_items
  try {
    const { productRows } = mapShopifyOrderToBitrixDeal(order);
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] Updating product rows for deal ${dealId}: ${productRows?.length || 0} rows`);
    
    // Always call productrows.set - even if empty array (to clear removed items)
      await callBitrix('/crm.deal.productrows.set.json', {
        id: dealId,
      rows: productRows || [],
      });
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] ✅ Product rows updated for deal ${dealId}: ${productRows?.length || 0} rows`);
  } catch (productRowsError) {
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] ❌ Product rows update error:`, productRowsError);
    // Don't throw - deal fields are already updated
  }

  return dealId;
}

/**
 * Handle product updated event - update internal product catalog only
 * This handler updates SKU/handle → PRODUCT_ID mapping, Brand, Size, etc.
 * DOES NOT touch deals - only internal product reference data
 */
async function handleProductUpdated(product) {
  console.log(`[SHOPIFY WEBHOOK] Handling product updated: ${product.id || product.title}`);
  console.log(`[SHOPIFY WEBHOOK] Product data:`, {
    id: product.id,
    title: product.title,
    handle: product.handle,
    vendor: product.vendor,
    variants: product.variants?.length || 0
  });

  // TODO: Update internal product catalog (SKU/handle → PRODUCT_ID mapping)
  // This could update a database, file, or configuration
  // For now, just log the update
  // Example structure:
  // - Update SKU_TO_PRODUCT_ID mapping in config or external storage
  // - Store product metadata (Brand, Size, Model, etc.)
  // - Do NOT modify any deals
  
  if (product.variants && Array.isArray(product.variants)) {
    product.variants.forEach((variant, index) => {
      console.log(`[SHOPIFY WEBHOOK] Product variant ${index + 1}:`, {
        id: variant.id,
        sku: variant.sku,
        title: variant.title,
        price: variant.price,
        inventory_quantity: variant.inventory_quantity
      });
      // TODO: Update SKU mapping here
      // Example: updateProductMapping(variant.sku, { productId, brand, size, etc. })
    });
  }

  console.log(`[SHOPIFY WEBHOOK] ✅ Product catalog update processed (no deals affected)`);
  return true;
}

/**
 * Handle refund created event - update deal in Bitrix
 * NOTE: DISABLED - Using orders/updated webhook instead (no admin API calls)
 * Shopify refund webhook sends refund object, but we process refunds via orders/updated
 * which contains full order data with updated financial_status
 */
/*
async function handleRefundCreated(refundData) {
  console.log(`[SHOPIFY WEBHOOK] ⚠️ handleRefundCreated is disabled - refunds are handled by orders/updated webhook`);
  // DISABLED: Using orders/updated webhook instead to avoid admin API calls
  // Refunds are automatically handled when orders/updated webhook fires with updated financial_status
  return null;
}
*/

// Export handler function for reuse in other endpoints
export async function handler(req, res) {
  // Enhanced logging - log ALL incoming requests immediately
  const requestId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  console.log(`[SHOPIFY WEBHOOK] ===== INCOMING REQUEST [${requestId}] =====`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Timestamp: ${new Date().toISOString()}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Method: ${req.method}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] URL: ${req.url}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] All headers:`, JSON.stringify(req.headers, null, 2));
  
  if (req.method !== 'POST') {
    console.log(`[SHOPIFY WEBHOOK] [${requestId}] ❌ Method not allowed: ${req.method}`);
    res.status(405).end('Method not allowed');
    return;
  }

  // Log raw body size
  const bodyString = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Body size: ${bodyString?.length || 0} bytes`);
  
  const topic = req.headers['x-shopify-topic'] || req.headers['X-Shopify-Topic'];
  const shopifyShopDomain = req.headers['x-shopify-shop-domain'] || req.headers['X-Shopify-Shop-Domain'];
  const shopifyHmac = req.headers['x-shopify-hmac-sha256'] || req.headers['X-Shopify-Hmac-Sha256'];
  
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Topic: ${topic || 'MISSING!'}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Shop Domain: ${shopifyShopDomain || 'MISSING!'}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] HMAC Present: ${!!shopifyHmac}`);
  
  const order = req.body;

  // Try to extract order info even if structure is different
  const orderId = order?.id || order?.order_id || order?.order?.id || 'N/A';
  const orderName = order?.name || order?.order_name || order?.order?.name || 'N/A';
  
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Order ID: ${orderId}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Order Name: ${orderName}`);
  console.log(`[SHOPIFY WEBHOOK] [${requestId}] Body keys: ${Object.keys(order || {}).join(', ')}`);

  // If no topic, log full body for debugging
  if (!topic) {
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] ⚠️ NO TOPIC HEADER! Full body:`, JSON.stringify(order, null, 2));
  }

  try {
    // Store event for monitoring (non-blocking)
    try {
      const storedEvent = shopifyAdapter.storeEvent(order);
      console.log(`[SHOPIFY WEBHOOK] [${requestId}] ✅ Event stored. Topic: ${topic}, Order: ${orderName || orderId}`);
    } catch (storeError) {
      console.error(`[SHOPIFY WEBHOOK] [${requestId}] ⚠️ Failed to store event:`, storeError);
    }

    // Handle different topics - SEPARATE HANDLERS
    if (topic === 'orders/create') {
      await handleOrderCreated(order, requestId);
    } else if (topic === 'orders/updated') {
      // orders/updated handles all updates including refunds and cancellations
      await handleOrderUpdated(order, requestId);
    } else if (topic === 'products/update') {
      // Product updates only affect internal catalog, not deals
      await handleProductUpdated(order);
    } else if (topic === 'refunds/create') {
      // Refunds are handled by orders/updated - this is deprecated but kept for backward compatibility
      console.log(`[SHOPIFY WEBHOOK] [${requestId}] ⚠️ refunds/create webhook received but refunds are handled by orders/updated`);
      // Optionally: fetch order and process via handleOrderUpdated
      // For now, just log and return 200
    } else {
      // For other topics just log and return 200
      console.log(`[SHOPIFY WEBHOOK] [${requestId}] Unhandled topic: ${topic || 'null/undefined'}`);
    }

    console.log(`[SHOPIFY WEBHOOK] [${requestId}] ✅ Request processed successfully`);
    res.status(200).json({ success: true, requestId, topic });
  } catch (e) {
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] ❌ Error:`, e);
    console.error(`[SHOPIFY WEBHOOK] [${requestId}] Error stack:`, e.stack);
    res.status(500).json({ 
      error: 'Internal server error', 
      message: e.message,
      requestId 
    });
  }
}

// Default export for direct use
export default handler;
