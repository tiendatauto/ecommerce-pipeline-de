# Databricks notebook source
from datetime import datetime


def get_batch_id(widget_name="p_batch_id"):
    """Return the workflow batch id, with today's date as a manual-run fallback."""
    default_batch_id = datetime.now().strftime("%Y-%m-%d")
    dbutils.widgets.text(widget_name, default_batch_id)

    batch_id = dbutils.widgets.get(widget_name).strip()
    if not batch_id:
        raise ValueError(f"{widget_name} is missing")

    return batch_id
