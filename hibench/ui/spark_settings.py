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

"""The single canonical Spark base; run-specific overrides belong in Experiments."""
import uuid
import streamlit as st
from hibench import config, spark_configuration


def render():
    st.title('Spark configuration')
    st.caption('One shared Spark configuration for all workloads. Save shared properties and scale sections to conf/spark.conf; use Experiments for run-specific overrides.')
    target_update_status()
    state = spark_configuration.shared()
    from hibench.catalog import SCALES
    scale = st.selectbox('Spark scale', SCALES, index=SCALES.index(state['defaults']['scale']))
    base = config.default_experiment(scale=scale, selected_workloads=['micro.sleep'])
    st.write(f"**Master:** {base['target']['master']} · **Deploy mode:** {base['target']['deploy_mode']}")
    st.write(f"**Resources:** {base['resources']['executor_instances']} executors · {base['resources']['executor_cores']} cores per executor · {base['resources']['executor_memory']} executor memory · {base['resources']['driver_memory']} driver memory")
    st.caption('Common Spark settings -> selected scale -> experiment overrides. Missing scale keys inherit the common base. Spark scale sections are in conf/spark.conf; generator partitions are in conf/hibench.conf and do not change when Spark resources change.')
    values, sources = spark_configuration.configuration(scale=scale)
    st.dataframe([{'Property': key, 'Value': value}
                  for key, value in sorted(values.items()) if sources[key] == 'scale:' + scale], hide_index=True, width='stretch')
    st.caption('Each data scale has explicit initial execution settings. The selector previews its effective values; experiment overrides remain separate.')
    st.caption('Source: ' + state['configuration_file'])
    with st.form('shared-spark-' + state['sha256']):
        blocks = spark_configuration.sections(state['text'])
        scale_text = st.text_area('Spark properties for ' + scale, blocks.get(scale, ''), height=350, key='spark-scale-editor-' + scale + '-' + state['sha256'], help='One spark.* property and value per line. Add any Spark property here to apply it to this data size for all workloads. Do not add a section header.')
        common_lines = blocks['common'].splitlines()
        common_properties = '\n'.join(line for line in common_lines if line.strip() and not line.lstrip().startswith('#'))
        common_comments = '\n'.join(line for line in common_lines if line.lstrip().startswith('#'))
        with st.expander('Edit common properties'):
            common_text = st.text_area('Common Spark properties', common_properties, height=180, key='spark-common-editor-' + state['sha256'], help='Settings shared by all sizes, such as event logging. Resource and execution settings belong to each data size.')
        saved = st.form_submit_button('Save and update target', type='primary', help=SAVE_HELP)
    if saved:
        try:
            overrides = config.overrides(st.session_state.experiment)
            text = spark_configuration.replace_sections(state['text'], {scale: scale_text, 'common': common_comments + '\n\n' + common_text})
            spark_configuration.save_shared(text, state['sha256'])
            st.session_state.experiment = config.resolve(overrides)
            st.session_state.request_key = str(uuid.uuid4())
            update_saved_target()
            st.rerun()
        except (ValueError, TypeError, OSError) as exc:
            st.error(str(exc))


SAVE_HELP = ('Validate and save the shared configuration in conf/, then copy configuration, launch scripts, '
             'the compiled Spark JAR and required resources to the target selected in Experiments. '
             'This makes the target use your saved changes. Compile Scala/Java changes first. '
             'No benchmark is started and the cluster is not restarted. If the connection fails, '
             'your local changes remain saved and you can retry the update.')


def update_saved_target():
    """Keep local saves even when the independent remote installation fails."""
    try:
        with st.spinner('Updating HiBench on target…'):
            from hibench.services.runtime import install
            install(st.session_state.experiment['target'])
        st.session_state['target-update-status'] = {'ok': True}
    except Exception as exc:
        st.session_state['target-update-status'] = {'ok': False, 'error': str(exc)}


def target_update_status():
    result = st.session_state.get('target-update-status')
    if not result:
        return
    if result['ok']:
        st.success('Configuration saved and HiBench updated on target.')
    else:
        st.warning('Configuration saved locally. Target update failed. The target has not been confirmed up to date.')
        st.error(result['error'])
        if st.button('Retry target update', help='Copy the currently saved configuration and runtime to the target selected in Experiments. This does not save editor changes or start a benchmark.'):
            update_saved_target()
            st.rerun()
