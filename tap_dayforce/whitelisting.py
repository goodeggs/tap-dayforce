from typing import Set

WHITELISTED_COLLECTIONS: Set = {"CompensationSummary", "EmploymentStatuses"}

WHITELISTED_FIELDS: Set = {
    "BaseRate",
    "BaseSalary",
    "PreviousBaseRate",
    "PreviousBaseSalary",
    "ChangeValue",
    "ChangePercent",
}

WHITELISTED_PAY_POLICY_CODES: Set = {"USA_CA_HNE", "USA_CA_HNE_4", "USA_CA_HNEWHSE", "USA_CA_HNEDRIVER"}
