from os import listdir, environ
from os.path import isfile, exists, join
import subprocess

def main():
    print(f"Starting Upload...")

    branch = environ.get("GIT_BRANCH")


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

                upload_file(pb_path, branch, test_group, param)

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

if __name__ == '__main__':
    main()
