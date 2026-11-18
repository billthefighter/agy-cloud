from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol

import httpx

from agent_tasks.tasks.food_tracker.models import GarminActivityRecord, GarminDailyMetric

FULL_DAY_MS = 86_400_000
GRAMS_PER_LB = 453.59237
DEFAULT_GARMIN_BASE_URL = "https://personal.connectapi.garmin.com"


class GarminAuthError(RuntimeError):
    """Raised when the Garmin Personal API rejects the API key or bearer token."""


@dataclass(slots=True)
class GarminSyncResult:
    """Normalized Garmin metrics, activities, and weight entries across a date window."""

    daily_metrics: list[GarminDailyMetric] = field(default_factory=list)
    weights_lbs: list[tuple[str, float]] = field(default_factory=list)


class GarminClient(Protocol):
    """Protocol for pulling daily metabolic burn, workouts, and weight from Garmin."""

    async def sync_range(
        self,
        *,
        start_date: str,
        end_date: str,
        fallback_bmr_kcal: float | None = None,
    ) -> GarminSyncResult: ...


def _decode_jwt_exp(token: str) -> float | None:
    """Read the `exp` claim from a JWT without external crypto dependencies."""
    parts = token.split(".")
    if len(parts) < 2:
        return None
    payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        raw = base64.urlsafe_b64decode(payload_b64.encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
        exp = payload.get("exp")
        return float(exp) if exp is not None else None
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return None


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_int(val: Any, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return round(float(val))
    except (TypeError, ValueError):
        return default


def _normalize_full_day_bmr(
    *,
    bmr_raw_kcal: float,
    duration_ms: int,
    is_completed_day: bool,
    fallback_bmr_kcal: float | None,
) -> float:
    """Normalize Garmin's intraday-prorated `bmrKilocalories` to a 24h baseline."""
    if is_completed_day or duration_ms >= FULL_DAY_MS:
        if bmr_raw_kcal > 0:
            return round(bmr_raw_kcal, 1)
        return round(fallback_bmr_kcal, 1) if fallback_bmr_kcal else 0.0

    # Today is still accruing hour-by-hour; project once >=1h has elapsed.
    if duration_ms >= 3_600_000 and bmr_raw_kcal > 0:
        return round(bmr_raw_kcal * (FULL_DAY_MS / float(duration_ms)), 1)
    if fallback_bmr_kcal and fallback_bmr_kcal > 0:
        return round(fallback_bmr_kcal, 1)
    return round(bmr_raw_kcal, 1)


def _iter_dates(start_date: str, end_date: str) -> list[str]:
    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)
    if end_d < start_d:
        return []
    days = (end_d - start_d).days + 1
    return [(start_d + timedelta(days=i)).isoformat() for i in range(days)]


class GarminPersonalClient:
    """Async client for the Garmin Connect Personal API (`personal.connectapi.garmin.com`)."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = DEFAULT_GARMIN_BASE_URL,
        http_transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 20.0,
    ) -> None:
        self.api_key = api_key.strip()
        self.base_url = (base_url or DEFAULT_GARMIN_BASE_URL).rstrip("/")
        self._http_transport = http_transport
        self._timeout = timeout
        self._bearer_token: str | None = None
        self._token_expires_at: float = 0.0

    def _make_client(self) -> httpx.AsyncClient:
        kwargs: dict[str, Any] = {"timeout": self._timeout, "follow_redirects": True}
        if self._http_transport is not None:
            kwargs["transport"] = self._http_transport
        return httpx.AsyncClient(**kwargs)

    async def _fetch_access_token(self, client: httpx.AsyncClient) -> str:
        if not self.api_key:
            raise GarminAuthError("GARMIN_API_KEY is empty.")
        url = f"{self.base_url}/personalapitoken-service/personal-api/personal-api-key/token"
        resp = await client.post(url, headers={"Authorization": f"ApiKey {self.api_key}"})
        if resp.status_code in (401, 403):
            raise GarminAuthError(
                f"Garmin Personal API key rejected (HTTP {resp.status_code}). "
                "Verify or rotate GARMIN_API_KEY in Garmin Connect Settings."
            )
        resp.raise_for_status()
        token = resp.text.strip().strip('"').strip("'")
        if not token:
            raise GarminAuthError("Garmin token endpoint returned an empty token.")
        exp = _decode_jwt_exp(token)
        now = time.time()
        self._token_expires_at = (exp - 300.0) if exp is not None else (now + 23 * 3600)
        self._bearer_token = token
        return token

    async def _get_token(self, client: httpx.AsyncClient, *, force_refresh: bool = False) -> str:
        if not force_refresh and self._bearer_token and time.time() < self._token_expires_at:
            return self._bearer_token
        return await self._fetch_access_token(client)

    async def _authed_get_json(
        self,
        client: httpx.AsyncClient,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        token = await self._get_token(client)
        resp = await client.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {token}"},
        )
        if resp.status_code == 401:
            token = await self._get_token(client, force_refresh=True)
            resp = await client.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {token}"},
            )
            if resp.status_code in (401, 403):
                raise GarminAuthError(
                    f"Garmin Personal API unauthorized after token refresh (HTTP {resp.status_code})."
                )
        if resp.status_code in (204, 400, 404) or not resp.content:
            return None
        resp.raise_for_status()
        return resp.json()

    async def _authed_get_optional(
        self,
        client: httpx.AsyncClient,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        try:
            return await self._authed_get_json(client, path, params=params)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (400, 404):
                return None
            raise

    async def sync_range(
        self,
        *,
        start_date: str,
        end_date: str,
        fallback_bmr_kcal: float | None = None,
    ) -> GarminSyncResult:
        """Pull daily metabolic summaries, workouts, and weight across `[start_date, end_date]`."""
        dates = _iter_dates(start_date, end_date)
        if not dates:
            return GarminSyncResult()

        now_iso = datetime.now(UTC).isoformat()
        async with self._make_client() as client:
            # 1. Fetch activities for the window (single request covers recent history).
            activities_by_date: dict[str, list[GarminActivityRecord]] = {d: [] for d in dates}
            act_limit = max(20, len(dates) * 5)
            raw_activities = await self._authed_get_json(
                client,
                "/activitylist-service/activities/search/activities",
                params={"start": 0, "limit": act_limit},
            )
            if isinstance(raw_activities, list):
                for item in raw_activities:
                    if not isinstance(item, dict):
                        continue
                    start_local = str(item.get("startTimeLocal") or "")
                    act_date = start_local[:10]
                    if act_date not in activities_by_date:
                        continue
                    act_type_obj = item.get("activityType")
                    type_key = (
                        str(act_type_obj.get("typeKey") or "other")
                        if isinstance(act_type_obj, dict)
                        else str(act_type_obj or "other")
                    )
                    activities_by_date[act_date].append(
                        GarminActivityRecord(
                            activity_id=_safe_int(item.get("activityId")),
                            name=str(
                                item.get("activityName") or type_key.replace("_", " ").title()
                            ),
                            activity_type=type_key,
                            calories_kcal=round(_safe_float(item.get("calories")), 1),
                            duration_seconds=round(_safe_float(item.get("duration")), 1),
                            distance_meters=round(_safe_float(item.get("distance")), 1),
                            average_hr=(
                                round(_safe_float(item.get("averageHR")), 1)
                                if item.get("averageHR") is not None
                                else None
                            ),
                            start_time_local=start_local,
                        )
                    )

            # 2. Fetch daily summaries chronologically so completed past days provide BMR fallback.
            metrics: list[GarminDailyMetric] = []
            running_bmr_fallback = fallback_bmr_kcal
            for d_str in dates:
                raw_daily = await self._authed_get_json(
                    client,
                    "/usersummary-service/usersummary/daily",
                    params={"calendarDate": d_str},
                )
                if not isinstance(raw_daily, dict):
                    continue
                bmr_raw = _safe_float(raw_daily.get("bmrKilocalories"))
                active_kcal = _safe_float(raw_daily.get("activeKilocalories"))
                duration_ms = _safe_int(raw_daily.get("durationInMilliseconds"))
                is_completed = d_str < end_date
                bmr_full = _normalize_full_day_bmr(
                    bmr_raw_kcal=bmr_raw,
                    duration_ms=duration_ms,
                    is_completed_day=is_completed,
                    fallback_bmr_kcal=running_bmr_fallback,
                )
                if bmr_full > 0 and (is_completed or duration_ms >= FULL_DAY_MS):
                    running_bmr_fallback = bmr_full

                # Skip empty placeholder days with zero wellness data and zero activities.
                acts = activities_by_date.get(d_str, [])
                if (
                    bmr_raw <= 0
                    and active_kcal <= 0
                    and _safe_int(raw_daily.get("totalSteps")) <= 0
                    and not acts
                ):
                    continue

                total_burn = round(bmr_full + active_kcal, 1)
                resting_hr_raw = raw_daily.get("restingHeartRate")
                rhr_7d_raw = raw_daily.get("lastSevenDaysAvgRestingHeartRate")
                bb_high_raw = raw_daily.get("bodyBatteryHighestValue")
                bb_low_raw = raw_daily.get("bodyBatteryLowestValue")
                bb_cur_raw = raw_daily.get("bodyBatteryMostRecentValue")
                bb_chg_raw = raw_daily.get("bodyBatteryChargedValue")
                stress_raw = raw_daily.get("averageStressLevel")
                sleep_sec_raw = raw_daily.get("measurableAsleepDuration") or raw_daily.get(
                    "sleepingSeconds"
                )
                step_goal_raw = raw_daily.get("dailyStepGoal")
                intensity_goal_raw = raw_daily.get("intensityMinutesGoal")

                sleep_deep: int | None = None
                sleep_rem: int | None = None
                sleep_score: int | None = None
                sleep_qual: str | None = None
                hrv_last: int | None = None
                hrv_week: int | None = None
                hrv_stat: str | None = None

                raw_sleep = await self._authed_get_optional(
                    client,
                    "/sleep-service/sleep/dailySleep",
                    params={"date": d_str},
                )
                if isinstance(raw_sleep, dict):
                    s_total = raw_sleep.get("sleepTimeSeconds")
                    if s_total is not None and _safe_int(s_total) > 0:
                        sleep_sec_raw = s_total
                    if raw_sleep.get("deepSleepSeconds") is not None:
                        sleep_deep = _safe_int(raw_sleep.get("deepSleepSeconds"))
                    if raw_sleep.get("remSleepSeconds") is not None:
                        sleep_rem = _safe_int(raw_sleep.get("remSleepSeconds"))
                    scores = raw_sleep.get("sleepScores")
                    if isinstance(scores, dict) and isinstance(scores.get("overall"), dict):
                        overall = scores["overall"]
                        if overall.get("value") is not None:
                            sleep_score = _safe_int(overall.get("value"))
                        if overall.get("qualifierKey"):
                            sleep_qual = str(overall["qualifierKey"])

                raw_hrv = await self._authed_get_optional(
                    client,
                    f"/hrv-service/hrv/{d_str}",
                )
                if isinstance(raw_hrv, dict) and isinstance(raw_hrv.get("hrvSummary"), dict):
                    hs = raw_hrv["hrvSummary"]
                    if hs.get("lastNightAvg") is not None:
                        hrv_last = _safe_int(hs.get("lastNightAvg"))
                    if hs.get("weeklyAvg") is not None:
                        hrv_week = _safe_int(hs.get("weeklyAvg"))
                    if hs.get("status"):
                        hrv_stat = str(hs["status"])

                metrics.append(
                    GarminDailyMetric(
                        local_date=d_str,
                        bmr_raw_kcal=round(bmr_raw, 1),
                        bmr_full_day_kcal=bmr_full,
                        active_kcal=round(active_kcal, 1),
                        total_burn_kcal=total_burn,
                        steps=_safe_int(raw_daily.get("totalSteps")),
                        resting_hr=_safe_int(resting_hr_raw)
                        if resting_hr_raw is not None
                        else None,
                        moderate_min=_safe_int(raw_daily.get("moderateIntensityMinutes")),
                        vigorous_min=_safe_int(raw_daily.get("vigorousIntensityMinutes")),
                        duration_ms=duration_ms,
                        activities=acts,
                        synced_at=now_iso,
                        step_goal=_safe_int(step_goal_raw, 10000) or 10000,
                        intensity_goal_min=_safe_int(intensity_goal_raw, 150) or 150,
                        resting_hr_7d_avg=(
                            _safe_int(rhr_7d_raw) if rhr_7d_raw is not None else None
                        ),
                        body_battery_high=(
                            _safe_int(bb_high_raw) if bb_high_raw is not None else None
                        ),
                        body_battery_low=(
                            _safe_int(bb_low_raw) if bb_low_raw is not None else None
                        ),
                        body_battery_current=(
                            _safe_int(bb_cur_raw) if bb_cur_raw is not None else None
                        ),
                        body_battery_charged=(
                            _safe_int(bb_chg_raw) if bb_chg_raw is not None else None
                        ),
                        avg_stress=(
                            _safe_int(stress_raw)
                            if stress_raw is not None and _safe_int(stress_raw) >= 0
                            else None
                        ),
                        sleep_seconds=(
                            _safe_int(sleep_sec_raw)
                            if sleep_sec_raw is not None and _safe_int(sleep_sec_raw) > 0
                            else None
                        ),
                        sleep_deep_seconds=sleep_deep,
                        sleep_rem_seconds=sleep_rem,
                        sleep_score=sleep_score,
                        sleep_qualifier=sleep_qual,
                        hrv_last_night_avg=hrv_last,
                        hrv_weekly_avg=hrv_week,
                        hrv_status=hrv_stat,
                    )
                )

            # 3. Fetch body weight entries (Garmin stores weight in grams).
            weights_lbs: list[tuple[str, float]] = []
            raw_weights = await self._authed_get_json(
                client,
                f"/weight-service/weight/range/{start_date}/{end_date}",
            )
            if isinstance(raw_weights, dict):
                summaries = raw_weights.get("dailyWeightSummaries") or []
                if isinstance(summaries, list):
                    for ws in summaries:
                        if not isinstance(ws, dict):
                            continue
                        latest = ws.get("latestWeight")
                        if not isinstance(latest, dict):
                            continue
                        w_grams = _safe_float(latest.get("weight"))
                        w_date = str(
                            latest.get("calendarDate") or ws.get("summaryDate") or ""
                        ).strip()
                        if w_grams > 0 and len(w_date) == 10:
                            w_lbs = round(w_grams / GRAMS_PER_LB, 2)
                            if 40.0 < w_lbs < 800.0:
                                weights_lbs.append((w_date, w_lbs))
            weights_lbs.sort(key=lambda item: item[0])

        return GarminSyncResult(daily_metrics=metrics, weights_lbs=weights_lbs)
