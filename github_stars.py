from prefect import flow, task
import httpx

@task(log_prints=True)
def get_stars(repo: str):
    url = f"https://api.github.com/repos/{repo}"
    response = httpx.get(url)
    response.raise_for_status()
    count = response.json()["stargazers_count"]
    print(f"Repository {repo} có {count} sao trên GitHub!")
    return count

@flow(name="GitHub Stars Flow")
def github_stars(repos: list[str]):
    results = []
    for repo in repos:
        star_count = get_stars(repo)
        results.append(star_count)
    return results

if __name__ == "__main__":
    github_stars(["PrefectHQ/prefect", "python/cpython"])
