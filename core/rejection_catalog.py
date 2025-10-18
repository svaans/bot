"""Catálogo normalizado de códigos de rechazo y utilidades asociadas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Mapping

DEFAULT_LOCALE = "es"


@dataclass(frozen=True)
class RejectionReason:
    """Metadatos de un código de rechazo estructurado."""

    code: str
    category: str
    severity: str
    default_message: str
    translations: Mapping[str, str] = field(default_factory=dict)

    def message_for(self, locale: str | None = None) -> str:
        """Retorna la descripción traducida para ``locale`` si existe."""

        if not locale:
            locale = DEFAULT_LOCALE
        normalized = locale.lower()
        if normalized in self.translations:
            return self.translations[normalized]
        short_locale = normalized.split("-")[0]
        if short_locale in self.translations:
            return self.translations[short_locale]
        return self.default_message


@dataclass(frozen=True)
class ResolvedRejection:
    """Representa un rechazo enriquecido con mensaje y metadatos."""

    reason: RejectionReason
    message: str
    detail: str | None
    locale: str

    @property
    def code(self) -> str:
        return self.reason.code

    @property
    def category(self) -> str:
        return self.reason.category

    @property
    def severity(self) -> str:
        return self.reason.severity

    def to_log_dict(self) -> Dict[str, object]:
        """Genera un diccionario listo para agregar como ``extra`` en logs."""

        payload: Dict[str, object] = {
            "reason_code": self.code,
            "reason_category": self.category,
            "reason_severity": self.severity,
            "reason_message": self.message,
            "reason_locale": self.locale,
        }
        if self.detail:
            payload["reason_detail"] = self.detail
        return payload


UNKNOWN_REJECTION = RejectionReason(
    code="UNKNOWN",
    category="unknown",
    severity="warning",
    default_message="Rechazo no clasificado",
    translations={
        "es": "Rechazo no clasificado",
        "en": "Unclassified rejection",
    },
)


REJECTION_CATALOG: Dict[str, RejectionReason] = {
    UNKNOWN_REJECTION.code: UNKNOWN_REJECTION,
    "COOLDOWN_ACTIVE": RejectionReason(
        code="COOLDOWN_ACTIVE",
        category="safeguard",
        severity="info",
        default_message="Cooldown activo",
        translations={
            "es": "Cooldown activo",
            "en": "Cooldown active",
        },
    ),
    "RISK_GUARD": RejectionReason(
        code="RISK_GUARD",
        category="risk",
        severity="warning",
        default_message="Gestor de riesgo bloqueó la entrada",
        translations={
            "es": "Gestor de riesgo bloqueó la entrada",
            "en": "Risk guard prevented the entry",
        },
    ),
    "CAPITAL_GUARD": RejectionReason(
        code="CAPITAL_GUARD",
        category="capital",
        severity="warning",
        default_message="Sin capital disponible",
        translations={
            "es": "Sin capital disponible",
            "en": "No free capital available",
        },
    ),
    "TECHNICAL_FILTER": RejectionReason(
        code="TECHNICAL_FILTER",
        category="strategy",
        severity="info",
        default_message="Filtros técnicos no aprobados",
        translations={
            "es": "Filtros técnicos no aprobados",
            "en": "Technical filters not satisfied",
        },
    ),
    "DUPLICATE_SIGNAL": RejectionReason(
        code="DUPLICATE_SIGNAL",
        category="integrity",
        severity="info",
        default_message="Se descartó señal duplicada",
        translations={
            "es": "Se descartó señal duplicada",
            "en": "Duplicate signal discarded",
        },
    ),
}


def get_rejection_reason(code: str | None) -> RejectionReason:
    """Obtiene un :class:`RejectionReason` conocido o ``UNKNOWN`` como fallback."""

    if not code:
        return UNKNOWN_REJECTION
    normalized = code.strip().upper()
    return REJECTION_CATALOG.get(normalized, UNKNOWN_REJECTION)


def resolve_rejection(
    motivo: str | RejectionReason | None,
    *,
    code: str | None = None,
    locale: str | None = None,
) -> ResolvedRejection:
    """Enriquece ``motivo`` con metadata estructurada."""

    selected_locale = (locale or DEFAULT_LOCALE).lower()
    detail = (motivo.default_message if isinstance(motivo, RejectionReason) else None)
    if not detail:
        detail_text = (str(motivo).strip() if motivo else "")
        detail = detail_text or None
    if isinstance(motivo, RejectionReason):
        reason = motivo
    else:
        reason = get_rejection_reason(code)
    message = reason.message_for(selected_locale)
    if reason is UNKNOWN_REJECTION and detail:
        message = detail
    return ResolvedRejection(reason=reason, message=message, detail=detail, locale=selected_locale)


__all__ = [
    "DEFAULT_LOCALE",
    "REJECTION_CATALOG",
    "RejectionReason",
    "ResolvedRejection",
    "UNKNOWN_REJECTION",
    "get_rejection_reason",
    "resolve_rejection",
]