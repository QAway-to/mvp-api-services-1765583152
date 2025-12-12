// Static endpoint for refund create webhook
// Route: /api/webhook/refund/crt
export { config } from '../shopify.js';
import { handler as shopifyHandler } from '../shopify.js';

export default async function handler(req, res) {
  // Set topic header to refunds/create for the main handler
  req.headers['x-shopify-topic'] = 'refunds/create';
  return shopifyHandler(req, res);
}

