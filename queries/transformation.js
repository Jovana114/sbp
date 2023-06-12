db.product_info.aggregate([
  {
    $lookup: {
      from: "feedback",
      localField: "_id",
      foreignField: "product_id",
      as: "feedbacks"
    }
  },
  {
    $out: "product_with_feedbacks"
  }
])
