from __future__ import annotations

import typing as t

import rio

from .. import components as comps


@rio.page(
    name='Main Menu',
    url_segment='main-menu',
)
class MainMenu(rio.Component):
    def build(self) -> rio.Component:
        return rio.Column(
            rio.Text()


        )
