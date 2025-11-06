const mongoose = require('mongoose')

const variantSchema = new mongoose.Schema(
  {
    variant_id: { type: String, required: true },
    price_currency: { type: String, default: 'USD' },
    original_price: { type: Number, default: 0 },
    selling_price: { type: Number, default: 0 },
    sale_price: { type: Number, default: 0 },
    final_price: { type: Number, default: 0 },
    discount: { type: Number, default: 0 },
    link_url: { type: String, required: true },
    deeplink_url: { type: String, required: true },
    image_url: { type: String, default: '' },
    alternate_image_urls: [{ type: String }],
    is_on_sale: { type: Boolean, default: false },
    is_in_stock: { type: Boolean, default: false },
    size: { type: String, default: '' },
    color: { type: String, default: '' },
    mpn: { type: String, default: '' },
    ratings_count: { type: Number, default: 0 },
    average_ratings: { type: Number, default: 0 },
    review_count: { type: Number, default: 0 },
    operation_type: {
      type: String,
      enum: ['INSERT', 'UPDATE', 'DELETE', 'NO_CHANGE'],
      default: 'INSERT',
    },
  },
  { timestamps: true }
)

const productSchema = new mongoose.Schema(
  {
    parent_product_id: { type: String, required: true },
    name: { type: String, required: true },
    description: { type: String, default: '' },
    category: { type: String },
    retailer_domain: { type: String, required: true },
    brand: { type: String, required: true },
    gender: {
      type: String,
    },
    materials: { type: String },
    return_policy_link: { type: String, default: '' },
    return_policy: { type: String, default: '' },
    source: { type: String, default: '' },
    size_chart: { type: String, default: '' },
    available_bank_offers: { type: String, default: '' },
    available_coupons: { type: String, default: '' },
    operation_type: {
      type: String,
      enum: ['INSERT', 'UPDATE', 'DELETE', 'NO_CHANGE'],
      default: 'INSERT',
    },
    variants: [variantSchema],
  },
  { timestamps: true }
)

// Indexes for better query performance
productSchema.index({ parent_product_id: 1 })
productSchema.index({ retailer_domain: 1 })
variantSchema.index({ variant_id: 1 })

module.exports = mongoose.model('Product', productSchema)
