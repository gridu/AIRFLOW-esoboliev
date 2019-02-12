#!/usr/bin/env bash
echo {{ ti.xcom_pull("set_user") }}