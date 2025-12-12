// Static endpoint for order update webhook
// Route: /api/webhook/order/upd
export { config } from '../shopify.js';
import { handler as shopifyHandler } from '../shopify.js';

export default async function handler(req, res) {
  // Set topic header to orders/updated for the main handler
  req.headers['x-shopify-topic'] = 'orders/updated';
  return shopifyHandler(req, res);
}

