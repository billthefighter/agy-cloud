from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import pydantic

MealSlot = Literal["breakfast", "lunch", "dinner", "snack"]
ConfidenceLevel = Literal["low", "medium", "high"]
ItemSource = Literal["vision_estimate", "nutrition_label", "saved_food", "user_edit"]
VALID_MEAL_REACTIONS: frozenset[str] = frozenset({"🤮", "🤤", "💪"})


class EstimatedMealItem(pydantic.BaseModel):
    """Single ingredient or food component inside a meal estimate."""

    name: str = pydantic.Field(description="Short readable food/ingredient name.")
    quantity_desc: str = pydantic.Field(
        description="Portion description, e.g. '6 oz cooked' or '1 tbsp'."
    )
    estimated_grams: float | None = pydantic.Field(
        default=None,
        description="Estimated edible weight in grams, if known.",
    )
    calories_kcal: float = pydantic.Field(description="Estimated energy in kcal.")
    protein_g: float = pydantic.Field(description="Protein in grams.")
    carbs_g: float = pydantic.Field(description="Total carbohydrate in grams.")
    fat_g: float = pydantic.Field(description="Total fat in grams.")
    fiber_g: float = pydantic.Field(default=0.0, description="Dietary fiber in grams.")
    source: ItemSource = pydantic.Field(
        default="vision_estimate",
        description="Origin of estimate: vision_estimate, nutrition_label, saved_food, or user_edit.",
    )


class MealEstimateSchema(pydantic.BaseModel):
    """Structured output schema enforced via google-antigravity LocalAgentConfig.response_schema."""

    title: str = pydantic.Field(
        description="Concise meal title, e.g. 'Pan-seared salmon with rice'."
    )
    meal_slot: MealSlot = pydantic.Field(
        description="Meal category: breakfast, lunch, dinner, or snack."
    )
    local_date_override: str | None = pydantic.Field(
        default=None,
        description=(
            "YYYY-MM-DD override ONLY when the user explicitly states a past/specific date "
            "(e.g. 'yesterday dinner' or '2026-11-15'); otherwise null."
        ),
    )
    confidence: ConfidenceLevel = pydantic.Field(
        description="Overall portion/macro confidence: high, medium, or low."
    )
    items: list[EstimatedMealItem] = pydantic.Field(
        description="Itemized breakdown including visible foods and inferred oils/sauces/dressings."
    )
    assumptions_note: str = pydantic.Field(
        default="",
        description="Explicit summary of portion assumptions and hidden calories (oils, sauces, cuts).",
    )
    clarification_question: str = pydantic.Field(
        default="",
        description=(
            "Single brief follow-up question ONLY when confidence is low and ambiguity could swing "
            "the meal by >150 kcal; otherwise empty string."
        ),
    )


@dataclass(slots=True)
class NutritionTarget:
    effective_date: str
    calories_kcal: int
    protein_g: float
    carbs_g: float
    fat_g: float
    fiber_g: float
    notes: str | None = None
    updated_at: str = ""


@dataclass(slots=True)
class SavedFood:
    id: int
    slug: str
    name: str
    serving_desc: str
    serving_grams: float | None
    calories_kcal: float
    protein_g: float
    carbs_g: float
    fat_g: float
    fiber_g: float
    created_at: str


@dataclass(slots=True)
class MealItemRecord:
    id: int
    meal_entry_id: int
    saved_food_id: int | None
    name: str
    quantity_desc: str
    estimated_grams: float | None
    calories_kcal: float
    protein_g: float
    carbs_g: float
    fat_g: float
    fiber_g: float
    source: ItemSource


@dataclass(slots=True)
class MealEntryRecord:
    id: int
    logged_at: str
    local_date: str
    meal_slot: MealSlot
    title: str
    status: str
    raw_description: str
    photo_relpath: str | None
    slack_channel_id: str | None
    slack_thread_ts: str | None
    slack_card_ts: str | None
    model_used: str
    confidence: ConfidenceLevel
    assumptions_note: str | None
    clarification_question: str | None
    scale_factor: float
    total_calories_kcal: float
    total_protein_g: float
    total_carbs_g: float
    total_fat_g: float
    total_fiber_g: float
    updated_at: str
    reaction_emoji: str | None = None
    items: list[MealItemRecord] = field(default_factory=list)


@dataclass(slots=True)
class GarminActivityRecord:
    activity_id: int
    name: str
    activity_type: str
    calories_kcal: float
    duration_seconds: float
    distance_meters: float = 0.0
    average_hr: float | None = None
    start_time_local: str = ""


@dataclass(slots=True)
class GarminDailyMetric:
    local_date: str
    bmr_raw_kcal: float
    bmr_full_day_kcal: float
    active_kcal: float
    total_burn_kcal: float
    steps: int = 0
    resting_hr: int | None = None
    moderate_min: int = 0
    vigorous_min: int = 0
    duration_ms: int = 0
    activities: list[GarminActivityRecord] = field(default_factory=list)
    synced_at: str = ""
    step_goal: int = 10000
    intensity_goal_min: int = 150
    resting_hr_7d_avg: int | None = None
    body_battery_high: int | None = None
    body_battery_low: int | None = None
    body_battery_current: int | None = None
    body_battery_charged: int | None = None
    avg_stress: int | None = None
    sleep_seconds: int | None = None
    sleep_deep_seconds: int | None = None
    sleep_rem_seconds: int | None = None
    sleep_score: int | None = None
    sleep_qualifier: str | None = None
    hrv_last_night_avg: int | None = None
    hrv_weekly_avg: int | None = None
    hrv_status: str | None = None


@dataclass(slots=True)
class DynamicTargetInfo:
    base_calories_kcal: int
    effective_calories_kcal: int
    adjustment_kcal: int
    baseline_bmr_kcal: float
    baseline_active_kcal: float
    today_active_kcal: float
    workout_surplus_kcal: float
    goal_delta_kcal: int
    active_factor: float
    mode: str
    source: str


@dataclass(slots=True)
class DailySummary:
    local_date: str
    target: NutritionTarget
    meals: list[MealEntryRecord]
    total_calories_kcal: float
    total_protein_g: float
    total_carbs_g: float
    total_fat_g: float
    total_fiber_g: float
    remaining_calories_kcal: float
    remaining_protein_g: float
    remaining_carbs_g: float
    remaining_fat_g: float
    remaining_fiber_g: float
    garmin: GarminDailyMetric | None = None
    dynamic_target: DynamicTargetInfo | None = None


@dataclass(slots=True)
class WeeklySummary:
    start_date: str
    end_date: str
    days_logged: int
    target: NutritionTarget
    avg_calories_kcal: float
    avg_protein_g: float
    avg_carbs_g: float
    avg_fat_g: float
    avg_fiber_g: float
    protein_hit_days: int
    calorie_adherent_days: int
    daily_totals: list[DailySummary]
    avg_garmin_burn_kcal: float | None = None
    avg_garmin_bmr_kcal: float | None = None
    avg_garmin_active_kcal: float | None = None
    garmin_days_synced: int = 0


@dataclass(slots=True)
class BodyMetricRecord:
    local_date: str
    weight_lbs: float
    ema_weight_lbs: float
    notes: str | None
    logged_at: str


@dataclass(slots=True)
class AdaptiveTdeeEstimate:
    window_days: int
    days_logged: int
    avg_daily_kcal: float
    start_ema_lbs: float
    end_ema_lbs: float
    weight_delta_lbs: float
    span_days: int
    estimated_tdee_kcal: float
