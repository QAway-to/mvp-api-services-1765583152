// Static endpoint for order creation webhook
// Route: /api/webhook/order/crt
export { config } from '../shopify.js';
import { handler as shopifyHandler } from '../shopify.js';

export default async function handler(req, res) {
  // Set topic header to orders/create for the main handler
  req.headers['x-shopify-topic'] = 'orders/create';
  return shopifyHandler(req, res);
}
