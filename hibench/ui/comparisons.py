# Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import streamlit as st
from hibench import store
from hibench.services import comparisons


def render():
    st.caption('Compare implementations within one run or observations from two completed runs. A is the baseline; ratios are B / A.')
    saved = comparisons.recent()
    selected = st.selectbox('Saved comparison', [None] + [r['comparison_id'] for r in saved],
                            key='saved-comparison', format_func=lambda value: 'New comparison' if value is None else value)
    if selected:
        analysis = comparisons.load(selected)
        statuses = comparisons.source_status(analysis)
        if any(status != 'Unchanged' for status in statuses):
            st.warning('Source evidence: A ' + statuses[0] + ', B ' + statuses[1] + '. Displaying the saved snapshot.')
    else:
        rows = [store.get(r['id']) for r in store.recent(limit=1000) if r['state'] in ('succeeded', 'failed', 'cancelled')]
        for key in ('comparison-run-A', 'comparison-run-B'):
            identifier = st.session_state.get(key)
            if identifier and identifier not in {r['id'] for r in rows}:
                try:
                    row = store.get(identifier)
                    if row['state'] in ('succeeded', 'failed', 'cancelled'):
                        rows.append(row)
                except ValueError:
                    st.session_state.pop(key, None)
        if not rows:
            st.info('Complete a run to compare its results.')
            return
        lookup = {r['id']: r for r in rows}
        selections = []
        for side, column in zip(('A', 'B'), st.columns(2)):
            with column:
                run = st.selectbox('Run ' + side, list(lookup), key='comparison-run-' + side,
                                   format_func=lambda identifier: lookup[identifier]['spec'].get('name', 'Experiment') + ' · ' + identifier)
                languages = ['scala', 'pyspark'] if lookup[run]['spec'].get('implementation') == 'both' else [lookup[run]['spec'].get('implementation', 'scala')]
                language = st.selectbox('Implementation ' + side, languages, index=len(languages) - 1 if side == 'B' else 0)
                selections.append((run, language))
        ids = sorted({w['id'] for run, _ in selections for w in lookup[run]['spec']['workloads']})
        workload = st.selectbox('Workload', [None] + ids, format_func=lambda value: value or 'All workloads')
        try:
            analysis = comparisons.analyze(selections[0][0], selections[1][0], selections[0][1], selections[1][1], workload)
        except ValueError as exc:
            st.warning(str(exc))
            return
        if st.button('Save comparison'):
            analysis = comparisons.save(analysis)
            st.success('Saved comparison: ' + analysis['comparison_id'])
    st.caption(analysis['scope'])
    if any(state != 'succeeded' for state in analysis['source_states']):
        st.warning('At least one source run failed or was cancelled. Only completed observations are summarized.')
    if analysis.get('comparison_id'):
        st.caption('Saved snapshot: ' + analysis['comparison_id'])
    st.subheader('Conditions')
    st.dataframe([{**r, 'left': json.dumps(r['left']), 'right': json.dumps(r['right'])} for r in analysis['conditions']], hide_index=True, width='stretch')
    st.subheader('Duration summary')
    artifacts = comparisons.artifacts(analysis)
    rows = [{'workload': r['workload'], 'A median (s)': r['left']['median_s'], 'B median (s)': r['right']['median_s'],
             'B / A': r['ratio'], 'Difference (%)': r['difference_pct'], 'Samples A': r['left']['count'], 'Samples B': r['right']['count'],
             'A stdev (s)': r['left']['stdev_s'], 'B stdev (s)': r['right']['stdev_s']} for r in analysis['summary']]
    st.dataframe(rows, hide_index=True, width='stretch')
    phases = analysis.get('phase_summary', [])
    if phases:
        with st.expander('Instrumented workload phases', expanded=True):
            st.caption('Compare only the same timer protocol. Phase durations differ from Spark SQL durations. Workload and lifecycle totals contain other phases and must not be added to them. Missing measurements are not zero.')
            st.dataframe([{'Workload': r['workload'], 'Protocol': r['protocol'], 'Phase': r['phase'],
                'A median (s)': r['left']['median_s'], 'B median (s)': r['right']['median_s'],
                'B / A': r['ratio'], 'Samples A': r['left']['count'], 'Samples B': r['right']['count']}
                for r in phases], hide_index=True, width='stretch')
    observations = [{**r, 'side': side.upper()} for side, data in zip(('a', 'b'), analysis['observations'].values()) for r in data]
    valid = [r for r in observations if r['included']]
    if valid:
        import altair as alt
        import pandas as pd
        data = pd.DataFrame(valid)
        points = alt.Chart(data).mark_circle(size=70).encode(x=alt.X('duration_s:Q', title='Duration including startup (s)'),
                  y='side:N', color='side:N', tooltip=['workload', 'side', 'duration_s', 'application_id'])
        median = alt.Chart(data).mark_tick(color='black', thickness=3, size=30).encode(x='median(duration_s):Q', y='side:N')
        st.altair_chart((points + median).facet(row='workload:N'), width='stretch')
        ratios = pd.DataFrame([r for r in rows if r['B / A'] is not None])
        if not ratios.empty:
            chart = alt.Chart(ratios).mark_circle(size=90).encode(x=alt.X('B / A:Q', title='Duration ratio B / A'), y='workload:N', tooltip=['workload', 'B / A', 'Difference (%)'])
            reference = alt.Chart(pd.DataFrame({'reference': [1]})).mark_rule(strokeDash=[4, 4]).encode(x='reference:Q')
            st.altair_chart(chart + reference, width='stretch')
    with st.expander('Individual observations'):
        st.dataframe(observations, hide_index=True, width='stretch')
        from hibench.ui.executions import open_run
        for identifier in dict.fromkeys(analysis['selection'][side] for side in ('left', 'right')):
            try:
                store.get(identifier)
                available = True
            except ValueError:
                available = False
            st.button('Open source run ' + identifier, key='comparison-source-' + identifier,
                      disabled=not available, on_click=open_run, args=(identifier,))
    st.subheader('Download')
    for filename, content in artifacts.items():
        mime = 'application/json' if filename.endswith('.json') else 'text/csv' if filename.endswith('.csv') else 'text/html'
        st.download_button(filename, content, filename, mime)
    st.download_button('Comparison bundle (ZIP)', comparisons.export(analysis), 'comparison.zip', 'application/zip')
