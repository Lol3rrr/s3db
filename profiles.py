from os import listdir, environ
from os.path import isfile, exists, join, getmtime
import subprocess
import time
from datetime import datetime


def main():
    print(f"Starting Upload...")

    branch = environ.get("GIT_BRANCH")

    output = (
        "## Profiling Links\n\n"
    )

    for project in listdir("target/criterion"):
        project_path = join("target/criterion", project)

        for test_group in listdir(project_path):
            test_path = join(project_path, test_group)
            if isfile(test_path):
                continue

            for param in listdir(test_path):
                param_path = join(test_path, param)
                if isfile(param_path):
                    continue

                profile_path = join(param_path, "profile")
                pb_path = join(profile_path, "profile.pb")

                if not exists(pb_path):
                    continue

                link = upload_file(pb_path, branch, test_group, param)
                output += f"[{test_group} - {param}]({link})\n"

    current_dateTime = datetime.now()
    output += f"\nDate: {current_dateTime}"

    with open("profile-report.md", mode='w') as f:
        f.write(output)

def upload_file(path, branch, name, param):
    print(f"Uploading: {path} - branch: {branch} - bench-name: {name} - param: {param}")


    subprocess.run(
        [
            "profilecli",
            "upload",
            "--extra-labels=project=s3db",
            f"--extra-labels=branch={branch}",
            f"--extra-labels=benchname={name}",
            f"--extra-labels=params={param}",
            path
        ],
    )

    mod_time = getmtime(path)
    mod_time_ms = mod_time * 1000
    from_ms = mod_time_ms - (60 * 1000)
    to_ms = mod_time_ms + (60 * 1000)

    return f"https://grafana.lol3r.com/d/adkqscb0pineof/s3db?orgId=3&from={from_ms:.0f}&to={to_ms:.0f}&var-branch={branch}&var-benchname={name}&var-benchparam={param}"

if __name__ == '__main__':
    main()
