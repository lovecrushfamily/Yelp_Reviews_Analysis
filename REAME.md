



## 2. Description of Data

The Yelp dataset is downloaded from Yelp Reviews website. In total, there are 5,200,000 user reviews, information on 174,000 business. we will focus on two tables which are business table and review table. Attributes of business table are as following:

* business_id: ID of the business 
* name: name of the business
* neighborhood 
* address: address of the business
* city: city of the business
* state: state of the business
* postal_code: postal code of the business
* latitude: latitude of the business
* longitude: longitude of the business
* stars: average rating of the business
* review_count: number of reviews received
* is_open: 1 if the business is open, 0 therwise
* categories: multiple categories of the business

Attribues of review table are as following:
* review_id: ID of the review
* user_id: ID of the user
* business_id: ID of the business
* stars: ratings of the business
* date: review date
* text: review from the user
* useful: number of users who vote a review as usefull
* funny: number of users who vote a review as funny
* cool: number of users who vote a review as cool


## 3. Direction of Analysis

**Exploratory Data Analysis**

* Count something
* Vizualize something





| Column              | Meaning                                                         | Insight Potential                                                                 |
|----------------------|-----------------------------------------------------------------|------------------------------------------------------------------------------------|
| **user_id**          | Unique ID of each Yelp user                                     | Used to join with review table                                                     |
| **name**             | Display name (can be duplicated)                                | Minor, mostly for visualization                                                    |
| **review_count**     | Number of reviews this user has written                         | Core measure of user activity                                                      |
| **yelping_since**    | Date when the user joined Yelp                                  | Used to study user longevity and loyalty                                           |
| **useful, funny, cool** | Total number of votes received across all reviews             | Social validation — how engaging their reviews are                                 |
| **fans**             | Number of followers (people who marked this user as a favorite) | Measure of influence                                                               |
| **elite**            | Years when the user was recognized as “Elite” by Yelp (string list) | Loyalty and quality signal                                                    |
| **average_stars**    | The average rating this user gives to businesses                | User bias or sentiment tendency                                                    |
| **friends**          | Comma-separated list of user IDs this user befriends            | Social network potential                                                           |
| **compliment_***     | Number of times other users complimented them for that trait    | Fine-grained engagement metrics — e.g., `compliment_photos` = appreciated photo reviews |
