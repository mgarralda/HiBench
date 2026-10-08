"""Create a local, recoverable source snapshot without committing or pushing."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile


ROOT = Path(__file__).resolve().parents[1]


def git(*args):
    return subprocess.check_output(['git', '-C', str(ROOT), *args])


def main():
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    destination = ROOT / 'report' / 'reproducibility' / stamp
    destination.mkdir(parents=True, exist_ok=False)
    names = sorted(set(git('ls-files', '--cached', '--others', '--exclude-standard', '-z').decode('utf-8').split('\0')) - {''})
    excluded = {'.git', '.aws', '.codex', '.agents', '.venv', 'target', 'report', '__pycache__'}
    files = {}
    deleted = []
    archive = destination / 'hibench-next-source.zip'
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as output:
        for name in names:
            relative = Path(name)
            if relative.is_absolute() or '..' in relative.parts or excluded.intersection(relative.parts):
                continue
            path = ROOT / relative
            if ROOT not in path.resolve().parents:
                raise ValueError(f'Path outside source root: {name}')
            if not path.exists():
                deleted.append(name)
                continue
            if not path.is_file():
                continue
            data = path.read_bytes()
            files[name] = hashlib.sha256(data).hexdigest()
            output.writestr('source/' + relative.as_posix(), data)
        artifacts = {}
        for name in ('autogen/target/autogen-8.0-SNAPSHOT-jar-with-dependencies.jar',
                     'common/target/hibench-common-8.0-SNAPSHOT-jar-with-dependencies.jar',
                     'sparkbench/assembly/target/sparkbench-assembly-8.0-SNAPSHOT-dist.jar'):
            path = ROOT / name
            if path.is_file():
                artifacts[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest = {'schema_version': 1, 'created_utc': stamp,
                    'baseline_revision': git('rev-parse', 'HEAD').decode().strip(),
                    'branch': git('branch', '--show-current').decode().strip(),
                    'files': files, 'deleted': deleted, 'runtime_artifact_hashes': artifacts,
                    'excluded_directory_names': sorted(excluded)}
        output.writestr('snapshot-manifest.json', json.dumps(manifest, indent=2))
        output.writestr('working-tree.patch', git('diff', 'HEAD', '--binary'))
        output.writestr('git-status.txt', git('status', '--short'))
    # Verify bytes in the completed archive, not only the input files.
    with zipfile.ZipFile(archive) as check:
        for name, digest in files.items():
            assert hashlib.sha256(check.read('source/' + name)).hexdigest() == digest
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    (destination / 'snapshot.sha256').write_text(digest + '  ' + archive.name + '\n', encoding='utf-8')
    print(json.dumps({'archive': str(archive), 'sha256': digest, 'source_files': len(files),
                      'deleted_paths': len(deleted)}, indent=2))


if __name__ == '__main__':
    main()
