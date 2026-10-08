"""Static reference data the simulator draws from: dishes per cuisine, promotions, festivals."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Dish:
    name: str
    category: str  # main | starter | side | dessert | beverage | combo
    is_veg: bool
    price_factor: float  # multiple of (price_for_two / 2)
    prep_minutes: float


def _d(name: str, category: str, veg: bool, price: float, prep: float) -> Dish:
    return Dish(name, category, veg, price, prep)


DISHES: dict[str, tuple[Dish, ...]] = {
    "North Indian": (
        _d("Paneer Butter Masala", "main", True, 0.85, 14),
        _d("Dal Makhani", "main", True, 0.70, 12),
        _d("Butter Chicken", "main", False, 1.00, 15),
        _d("Kadai Paneer", "main", True, 0.85, 14),
        _d("Butter Naan", "side", True, 0.15, 6),
        _d("Jeera Rice", "side", True, 0.40, 8),
        _d("Chole Bhature", "main", True, 0.55, 12),
        _d("Aloo Paratha", "main", True, 0.35, 10),
    ),
    "Chinese": (
        _d("Veg Hakka Noodles", "main", True, 0.60, 10),
        _d("Chicken Fried Rice", "main", False, 0.75, 10),
        _d("Veg Manchurian", "starter", True, 0.55, 11),
        _d("Chilli Chicken", "starter", False, 0.80, 12),
        _d("Schezwan Fried Rice", "main", True, 0.65, 10),
        _d("Spring Rolls", "starter", True, 0.45, 9),
        _d("Hot and Sour Soup", "starter", True, 0.35, 7),
    ),
    "Biryani": (
        _d("Chicken Dum Biryani", "main", False, 1.10, 18),
        _d("Mutton Biryani", "main", False, 1.40, 20),
        _d("Veg Biryani", "main", True, 0.85, 16),
        _d("Egg Biryani", "main", False, 0.90, 16),
        _d("Raita", "side", True, 0.12, 2),
        _d("Mirchi Ka Salan", "side", True, 0.20, 3),
    ),
    "South Indian": (
        _d("Masala Dosa", "main", True, 0.40, 9),
        _d("Idli Vada Combo", "combo", True, 0.35, 7),
        _d("Rava Dosa", "main", True, 0.45, 10),
        _d("Uttapam", "main", True, 0.40, 10),
        _d("Filter Coffee", "beverage", True, 0.15, 3),
        _d("Mini Meals", "combo", True, 0.70, 8),
    ),
    "Fast Food": (
        _d("Veg Burger", "main", True, 0.45, 7),
        _d("Chicken Burger", "main", False, 0.60, 8),
        _d("French Fries", "side", True, 0.30, 5),
        _d("Peri Peri Wrap", "main", False, 0.55, 7),
        _d("Loaded Nachos", "starter", True, 0.50, 6),
        _d("Cold Coffee", "beverage", True, 0.30, 3),
    ),
    "Pizzas": (
        _d("Margherita Pizza", "main", True, 0.80, 14),
        _d("Farmhouse Pizza", "main", True, 1.00, 15),
        _d("Pepperoni Pizza", "main", False, 1.15, 15),
        _d("Garlic Bread", "side", True, 0.35, 8),
        _d("Choco Lava Cake", "dessert", True, 0.30, 4),
    ),
    "Italian": (
        _d("Penne Arrabbiata", "main", True, 0.85, 13),
        _d("Spaghetti Aglio Olio", "main", True, 0.80, 12),
        _d("Chicken Alfredo Pasta", "main", False, 1.00, 14),
        _d("Bruschetta", "starter", True, 0.50, 7),
        _d("Tiramisu", "dessert", True, 0.55, 3),
    ),
    "Desserts": (
        _d("Gulab Jamun", "dessert", True, 0.20, 2),
        _d("Brownie Sundae", "dessert", True, 0.45, 4),
        _d("Rasmalai", "dessert", True, 0.30, 2),
        _d("Cheesecake Slice", "dessert", True, 0.55, 3),
    ),
    "Beverages": (
        _d("Masala Chai", "beverage", True, 0.12, 4),
        _d("Fresh Lime Soda", "beverage", True, 0.18, 3),
        _d("Mango Lassi", "beverage", True, 0.25, 3),
        _d("Iced Americano", "beverage", True, 0.30, 3),
    ),
    "Snacks": (
        _d("Samosa (2 pcs)", "starter", True, 0.12, 4),
        _d("Vada Pav", "main", True, 0.10, 4),
        _d("Pav Bhaji", "main", True, 0.40, 9),
        _d("Paneer Tikka Sandwich", "main", True, 0.35, 7),
    ),
    "Mughlai": (
        _d("Mutton Rogan Josh", "main", False, 1.30, 18),
        _d("Chicken Seekh Kebab", "starter", False, 0.80, 14),
        _d("Shahi Paneer", "main", True, 0.85, 14),
        _d("Sheermal", "side", True, 0.20, 6),
    ),
    "Tandoor": (
        _d("Tandoori Chicken (Half)", "starter", False, 0.95, 17),
        _d("Paneer Tikka", "starter", True, 0.80, 14),
        _d("Tandoori Roti", "side", True, 0.08, 5),
        _d("Malai Chicken Tikka", "starter", False, 0.95, 16),
    ),
    "Bakery": (
        _d("Chocolate Truffle Cake (500g)", "dessert", True, 1.30, 4),
        _d("Veg Puff", "starter", True, 0.12, 3),
        _d("Croissant", "starter", True, 0.25, 3),
        _d("Blueberry Muffin", "dessert", True, 0.20, 2),
    ),
    "Ice Cream": (
        _d("Belgian Chocolate Scoop", "dessert", True, 0.30, 2),
        _d("Kulfi Falooda", "dessert", True, 0.35, 3),
        _d("Sundae Cup", "dessert", True, 0.40, 3),
    ),
    "Seafood": (
        _d("Fish Curry Rice", "main", False, 1.10, 16),
        _d("Prawn Fry", "starter", False, 1.20, 14),
        _d("Fish Fingers", "starter", False, 0.85, 11),
    ),
    "Healthy Food": (
        _d("Quinoa Salad Bowl", "main", True, 0.85, 7),
        _d("Grilled Chicken Bowl", "main", False, 1.00, 10),
        _d("Cold-Pressed Juice", "beverage", True, 0.35, 2),
    ),
    "Thalis": (
        _d("Veg Thali", "combo", True, 0.80, 9),
        _d("Non-Veg Thali", "combo", False, 1.00, 11),
    ),
    "Asian": (
        _d("Pad Thai", "main", False, 1.00, 12),
        _d("Veg Dim Sum", "starter", True, 0.70, 11),
        _d("Thai Green Curry", "main", True, 1.00, 13),
        _d("Sushi Platter", "main", False, 1.50, 12),
    ),
    "American": (
        _d("BBQ Chicken Wings", "starter", False, 0.80, 13),
        _d("Mac and Cheese", "main", True, 0.75, 10),
        _d("Classic Hot Dog", "main", False, 0.55, 7),
    ),
    "Continental": (
        _d("Grilled Fish Lemon Butter", "main", False, 1.30, 16),
        _d("Chicken Steak", "main", False, 1.40, 17),
        _d("Caesar Salad", "starter", True, 0.70, 6),
    ),
    "Kebabs": (
        _d("Galouti Kebab", "starter", False, 1.00, 13),
        _d("Hara Bhara Kebab", "starter", True, 0.60, 11),
        _d("Chicken Shawarma Roll", "main", False, 0.45, 8),
    ),
    "Sweets": (
        _d("Kaju Katli (250g)", "dessert", True, 0.80, 2),
        _d("Jalebi with Rabri", "dessert", True, 0.35, 5),
    ),
}

# Cuisines from the Kaggle dataset that should reuse a richer dish list above.
CUISINE_ALIASES: dict[str, str] = {
    "Indian": "North Indian",
    "Punjabi": "North Indian",
    "Lucknowi": "Mughlai",
    "Hyderabadi": "Biryani",
    "Andhra": "South Indian",
    "Chettinad": "South Indian",
    "Kerala": "South Indian",
    "Street Food": "Snacks",
    "Chaat": "Snacks",
    "Burgers": "Fast Food",
    "Pastas": "Italian",
    "Cafe": "Beverages",
    "Juices": "Beverages",
    "Combo": "Thalis",
    "Salads": "Healthy Food",
    "Japanese": "Asian",
    "Thai": "Asian",
    "Pan-Asian": "Asian",
    "Tibetan": "Asian",
    "Arabian": "Kebabs",
    "Lebanese": "Kebabs",
    "Mexican": "American",
    "Barbecue": "Kebabs",
    "Grill": "Kebabs",
    "Waffle": "Desserts",
    "Keto": "Healthy Food",
}

FALLBACK_CUISINE = "North Indian"

# Relative preference of cuisines by part of day: breakfast, lunch, snacks, dinner, late night.
DAYPART_CUISINE_BOOST: dict[str, dict[str, float]] = {
    "breakfast": {"South Indian": 3.0, "Bakery": 2.5, "Beverages": 2.5, "Snacks": 1.8, "Healthy Food": 1.6},
    "lunch": {"Biryani": 1.5, "Thalis": 1.8, "North Indian": 1.3, "South Indian": 1.2, "Healthy Food": 1.3},
    "snacks": {"Snacks": 2.2, "Beverages": 2.0, "Bakery": 1.6, "Fast Food": 1.5, "Desserts": 1.4},
    "dinner": {"Biryani": 1.4, "North Indian": 1.3, "Chinese": 1.3, "Pizzas": 1.3, "Mughlai": 1.3},
    "late_night": {"Fast Food": 2.0, "Pizzas": 1.8, "Desserts": 1.8, "Biryani": 1.5, "Ice Cream": 1.7},
}


def daypart(hour: int) -> str:
    if 6 <= hour < 11:
        return "breakfast"
    if 11 <= hour < 16:
        return "lunch"
    if 16 <= hour < 19:
        return "snacks"
    if 19 <= hour < 23:
        return "dinner"
    return "late_night"


# Share of the day's orders placed in each hour (00..23), a lunch peak and a bigger dinner peak.
HOURLY_DEMAND_WEEKDAY = (
    0.55,
    0.30,
    0.12,
    0.05,
    0.03,
    0.04,
    0.18,
    0.55,
    1.10,
    1.30,
    1.00,
    1.60,
    3.60,
    4.10,
    2.90,
    1.60,
    1.40,
    1.80,
    2.60,
    4.30,
    5.20,
    4.40,
    2.60,
    1.30,
)
HOURLY_DEMAND_WEEKEND = (
    0.95,
    0.60,
    0.25,
    0.08,
    0.04,
    0.04,
    0.10,
    0.35,
    0.90,
    1.40,
    1.50,
    1.90,
    3.40,
    4.30,
    3.60,
    2.20,
    1.80,
    2.10,
    2.90,
    4.50,
    5.60,
    5.00,
    3.30,
    1.90,
)

WEEKDAY_FACTOR = (0.92, 0.90, 0.95, 0.98, 1.10, 1.22, 1.25)  # Monday..Sunday

# Indian festival dates that lift demand (and trigger the festival promotion).
FESTIVALS: dict[date, str] = {
    date(2026, 8, 28): "Raksha Bandhan",
    date(2026, 9, 14): "Ganesh Chaturthi",
    date(2026, 10, 20): "Dussehra",
    date(2026, 11, 8): "Diwali",
    date(2026, 12, 25): "Christmas",
    date(2026, 12, 31): "New Year's Eve",
    date(2027, 3, 22): "Holi",
}


@dataclass(frozen=True)
class Promotion:
    code: str
    description: str
    discount_type: str  # percent | flat
    value: int  # percent (e.g. 50) or flat amount in paise
    max_discount_paise: int
    min_order_paise: int
    funded_by: str  # platform | restaurant
    rule: str  # first_order | weekend | weekday_lunch | rain | festival | always
    valid_from: date
    valid_to: date | None


PROMOTIONS: tuple[Promotion, ...] = (
    Promotion(
        "WELCOME50",
        "50% off your first order, up to Rs 100",
        "percent",
        50,
        10_000,
        14_900,
        "platform",
        "first_order",
        date(2024, 1, 1),
        None,
    ),
    Promotion(
        "WEEKEND20",
        "20% off on weekends, up to Rs 80",
        "percent",
        20,
        8_000,
        29_900,
        "platform",
        "weekend",
        date(2024, 1, 1),
        None,
    ),
    Promotion(
        "LUNCH40",
        "Flat Rs 40 off weekday lunches",
        "flat",
        4_000,
        4_000,
        19_900,
        "platform",
        "weekday_lunch",
        date(2024, 1, 1),
        None,
    ),
    Promotion(
        "RAINYDAY30",
        "Flat Rs 30 off while it rains",
        "flat",
        3_000,
        3_000,
        19_900,
        "platform",
        "rain",
        date(2024, 1, 1),
        None,
    ),
    Promotion(
        "CHEF15",
        "15% off, funded by partner restaurants, up to Rs 60",
        "percent",
        15,
        6_000,
        24_900,
        "restaurant",
        "always",
        date(2024, 1, 1),
        None,
    ),
    Promotion(
        "FEAST100",
        "Flat Rs 100 off festival feasts",
        "flat",
        10_000,
        10_000,
        79_900,
        "platform",
        "festival",
        date(2026, 8, 1),
        None,
    ),
)

PAYMENT_METHODS = (("upi", 0.58), ("card", 0.18), ("wallet", 0.10), ("cod", 0.14))

# km/h in free-flowing traffic.
VEHICLE_SPEED_KMPH = {"bike": 24.0, "scooter": 23.0, "ev_scooter": 22.0, "bicycle": 12.0}
VEHICLE_MIX = (("bike", 0.68), ("scooter", 0.15), ("ev_scooter", 0.12), ("bicycle", 0.05))

# Shift templates in local minutes after midnight: (name, [(start, end), ...]).
SHIFT_TEMPLATES = (
    ("lunch", ((11 * 60, 16 * 60),)),
    ("dinner", ((18 * 60, 23 * 60 + 45),)),
    ("full_day", ((10 * 60, 22 * 60),)),
    ("split", ((11 * 60 + 30, 15 * 60), (18 * 60 + 30, 23 * 60 + 30))),
    ("morning", ((7 * 60, 13 * 60),)),
    ("late_night", ((19 * 60, 23 * 60 + 59),)),
    # covers both ends of the business day: the small hours and the late evening
    ("overnight", ((0, 3 * 60), (21 * 60, 23 * 60 + 59))),
)
SHIFT_MIX = (0.17, 0.25, 0.19, 0.17, 0.08, 0.08, 0.06)

TRAFFIC_LEVELS = ("low", "moderate", "high", "severe")
TRAFFIC_SPEED_FACTOR = (1.0, 0.85, 0.68, 0.52)
# Base traffic level by local hour.
TRAFFIC_BY_HOUR = (0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 2, 1, 1, 1, 1, 1, 1, 2, 3, 3, 2, 1, 1, 0)

CANCELLATION_REASONS = (
    "payment_failed",
    "restaurant_rejected",
    "customer_changed_mind",
    "customer_delay",
    "no_rider_available",
)
