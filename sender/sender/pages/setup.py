from __future__ import annotations

import json
import pathlib
import typing as t
from datetime import date

import rio

from .. import components as comps


@rio.page(
    name='Setup',
    url_segment='setup',
)
class Setup(rio.Component):
    value: date = date.today()
    STATE_FILE = pathlib.Path("../conf/configuration.json")
    def __init__(self):
        super().__init__()
        self._load_state()
        last_processed = self._load_state()
        #self.current_index = min(last_processed, len(self.data) - 1) if self.data else 0
    # марк


def _load_state(self) -> t.Optional[int]:
    """Загружает последний обработанный индекс из файла"""
    try:
        if self.STATE_FILE.exists():
            with open(self.STATE_FILE, "r") as f:
                state = json.load(f)
                return state.get("last_index", 0)
    except Exception as e:
        print(f"Ошибка загрузки состояния: {e}")
    return 0


def _save_state(self, index: int) -> None:
    """Сохраняет текущий индекс в файл"""
    try:
        with open(self.STATE_FILE, "w") as f:
            json.dump({"last_index": index}, f)
    except Exception as e:
        print(f"Ошибка сохранения состояния: {e}")



    def build(self) -> rio.Component:
        return rio.Column(
            rio.Row(
                rio.Text("URL сервера:"),
                rio.TextInput(),

            ),
            rio.Row(
                rio.Text('Дата начала отправки:'),
                rio.DateInput(value=self.bind().value,),
            ),
            rio.Row(rio.Text('Часовой пояс'),
                rio.TextInput(),

                ),



        )
