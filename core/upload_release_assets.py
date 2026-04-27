
def sync_release_assets(file_paths):

    import os
    import requests

    token = os.environ["GITHUB_TOKEN"]

    owner = "publiusTacitus"
    repo = "simulated_airline_database"

    # get latest release
    r = requests.get(
        f"https://api.github.com/repos/{owner}/{repo}/releases/latest",
        headers={"Authorization": f"token {token}"}
    )

    release = r.json()

    upload_url = (release["upload_url"].split("{")[0])

    existing_assets = {a["name"]:a["id"] for a in release["assets"]}

    for path in file_paths:

        name = os.path.basename(path)

        # delete old asset if exists
        if name in existing_assets:

            asset_id = existing_assets[name]

            requests.delete(
                f"https://api.github.com/repos/"
                f"{owner}/{repo}/releases/assets/{asset_id}",
                headers={"Authorization": f"token {token}"}
            )

        # upload new asset
        with open(path,"rb") as f:

            requests.post(
                (upload_url + f"?name={name}"),
                headers={
                   "Authorization": f"token {token}",
                   "Content-Type": "application/zip"
                }, data=f
            )

        print(f"{name} uploaded.")