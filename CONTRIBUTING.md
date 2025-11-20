# Contributing

Thanks for considering contribution!

### Setup

With [uv](https://docs.astral.sh/uv/getting-started/installation/) and pre-commit.
To initialize the uv environment and pre-commit, run

```bash
# uv sync  # Don't have to call sync explicitly
uv run pre-commit install
```

### Test

Run pre-commit/pre-push hooks:
```bash
uv run pre-commit run -a --hook-stage pre-commit
```

### Update

To update all packages:
```bash
$ uv lock --upgrade
```

## References

* [What is ThingsBoard?](https://thingsboard.io/docs/getting-started-guides/what-is-thingsboard/)
* [ThingsBoard API](https://thingsboard.io/docs/api/)
