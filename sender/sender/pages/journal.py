from __future__ import annotations

import typing as t

import rio

from .. import components as comps


@rio.page(
    name='Journal',
    url_segment='journal',
)
class Journal(rio.Component):
    def build(self) -> rio.Component:
        return rio.Markdown(
            '''
## This is a Sample Page

Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu
fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
culpa qui officia deserunt mollit anim id est laborum.
            '''
        )
