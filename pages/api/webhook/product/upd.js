// Static endpoint for product update webhook
// Route: /api/webhook/product/upd
export { config } from '../shopify.js';
import { handler as shopifyHandler } from '../shopify.js';

export default async function handler(req, res) {
  // Set topic header to products/update for the main handler
  req.headers['x-shopify-topic'] = 'products/update';
  return shopifyHandler(req, res);
}

