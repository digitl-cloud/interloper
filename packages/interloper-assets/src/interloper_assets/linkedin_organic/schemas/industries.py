import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Industries(Schema):
    """LinkedIn industry taxonomy (V2). One row per industry and day, with its localized names and its place in the hierarchy; resolves urn:li:industry values."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: int | None = Field(default=None, description="The industry identifier (the id of urn:li:industry).")
    name_localized_it_it: str | None = Field(default=None, description="The industry name in the it_IT locale.")
    name_localized_ru_ru: str | None = Field(default=None, description="The industry name in the ru_RU locale.")
    name_localized_pl_pl: str | None = Field(default=None, description="The industry name in the pl_PL locale.")
    name_localized_ro_ro: str | None = Field(default=None, description="The industry name in the ro_RO locale.")
    name_localized_tr_tr: str | None = Field(default=None, description="The industry name in the tr_TR locale.")
    name_localized_hi_in: str | None = Field(default=None, description="The industry name in the hi_IN locale.")
    name_localized_tl_ph: str | None = Field(default=None, description="The industry name in the tl_PH locale.")
    name_localized_pt_br: str | None = Field(default=None, description="The industry name in the pt_BR locale.")
    name_localized_th_th: str | None = Field(default=None, description="The industry name in the th_TH locale.")
    name_localized_ja_jp: str | None = Field(default=None, description="The industry name in the ja_JP locale.")
    name_localized_fr_fr: str | None = Field(default=None, description="The industry name in the fr_FR locale.")
    name_localized_in_id: str | None = Field(default=None, description="The industry name in the in_ID locale.")
    name_localized_cs_cz: str | None = Field(default=None, description="The industry name in the cs_CZ locale.")
    name_localized_de_de: str | None = Field(default=None, description="The industry name in the de_DE locale.")
    name_localized_ms_my: str | None = Field(default=None, description="The industry name in the ms_MY locale.")
    name_localized_zh_tw: str | None = Field(default=None, description="The industry name in the zh_TW locale.")
    name_localized_es_es: str | None = Field(default=None, description="The industry name in the es_ES locale.")
    name_localized_nl_nl: str | None = Field(default=None, description="The industry name in the nl_NL locale.")
    name_localized_sv_se: str | None = Field(default=None, description="The industry name in the sv_SE locale.")
    name_localized_da_dk: str | None = Field(default=None, description="The industry name in the da_DK locale.")
    name_localized_ko_kr: str | None = Field(default=None, description="The industry name in the ko_KR locale.")
    name_localized_en_us: str | None = Field(default=None, description="The industry name in the en_US locale.")
    name_localized_uk_ua: str | None = Field(default=None, description="The industry name in the uk_UA locale.")
    name_localized_zh_cn: str | None = Field(default=None, description="The industry name in the zh_CN locale.")
    name_localized_no_no: str | None = Field(default=None, description="The industry name in the no_NO locale.")
    name_localized_ar_ae: str | None = Field(default=None, description="The industry name in the ar_AE locale.")
    name_localized_ko_ko: str | None = Field(default=None, description="The industry name in the ko_KO locale.")
    parent_industries: str | None = Field(default=None, description="The parent industries (JSON array).")
    children_industries: str | None = Field(default=None, description="The child industries (JSON array).")
