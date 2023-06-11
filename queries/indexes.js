db.product_info.createIndex({ "feedback.is_recommended": 1 });
db.product_info.createIndex({ "brand_name": 1 });
db.product_info.createIndex({ "product_name": 1 });
db.product_info.createIndex({ "brand_id": 1 });
db.product_info.createIndex({ "_id": 1 });
db.product_info.createIndex({ "feedback.rating": 1 });
db.product_info.createIndex({
  "feedback.hair_color": 1,
  "feedback.skin_type": 1,
  "feedback.eye_color": 1
});
db.product_info.createIndex({ "tertiary_category": 1 });