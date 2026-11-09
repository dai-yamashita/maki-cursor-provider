# maki-cursor-provider

Cursor のサブスクリプションを、maki のモデルとして使うパッケージです。ツールの実行は maki が続けます。このパッケージは maki 本体には入りません。

中のプロキシは Cursor の非公開エージェント API を呼びます。Cursor は、利用者のトークンでこの種の非公開エンドポイントを呼ぶ第三者ツールを利用規約に反すると説明しています。入れるかどうかは、このリポジトリを使う人の判断です。

Node.js 22.6 以上が必要です。

## インストール

`~/.config/maki/init.lua` に次を書きます。公式の maki 0.7 はパッケージの展開先を `maki.plugin_dir` に出さないので、`package.path` を渡します。

```lua
maki.pack.add({
  "https://github.com/dai-yamashita/maki-cursor-provider",
}, {
  load = function(package)
    require("cursor").setup(package.path)
  end,
})
```

初回起動で、このパッケージの権限を承認します。承認のあと、ターミナルでログインします。

```sh
maki auth login cursor
```

ログインできたかは、次の 2 つで確認します。

```sh
maki auth status
maki models | grep '^cursor/'
```

`maki auth status` は provider を引数に取りません。`maki auth status cursor` は maki 0.7 では使えません。出力に `cursor` の行があれば登録されています。

モデルは `cursor/composer-2.5` です。Cursor のトークンは maki のプラグイン認証ストア（state ディレクトリの `auth/plugins/cursor.json`）だけに置きます。パッケージのチェックアウトにも、プロキシの state ディレクトリにも置きません。古い `credentials.json` は初回に一度だけ取り込み、取り込み後に削除します。会話の記録は state ディレクトリの `maki-cursor-provider/` に置くので、パッケージを更新しても消えません。

プロキシの依存関係は、ログイン時にチェックアウトの `proxy/` へ `npm ci` します。

## 利用枠と状態ディレクトリ

Cursor の「含まれる API 利用枠」が 100% になると、Cursor は auto バケット以外の
名前付きモデル（GLM / DeepSeek / Claude / GPT など）を `resource_exhausted` または
`not_found` で拒否します。プロキシは起動時に `GetCurrentPeriodUsage` を読み、枯渇していれば
上流へ送らずに 400 `insufficient_quota` とリセット日・代替モデルを返します。再試行はしません。
枠が回復するまで `cursor/auto` か `cursor/composer-2.5` を使ってください。

セッションが別のモデルを保存している場合は、明示的に上書きして再開します。

```sh
maki -m cursor/composer-2.5 -r <SESSION_ID>
```

会話の記録は state ディレクトリに保存し、7 日を過ぎたものと、上限を超えた古いものから削除します。

- `MAKI_CURSOR_CONVERSATIONS_MAX_FILES`（既定 5000）
- `MAKI_CURSOR_CONVERSATIONS_MAX_BYTES`（既定 2 GiB）

## 開発（テスト）

プロキシのテストは Node.js 22 以上で実行します。CI と同じコマンドです。

```sh
cd proxy
npm ci --omit=dev
node --experimental-strip-types --disable-warning=ExperimentalWarning --test --test-concurrency=1 test/*.test.ts
```

`--test-concurrency=1` は、h2 ブリッジのテストが実際のポートと子プロセスを使うためです。

## 困ったとき

`maki auth status` に `cursor` の行が出ないときは、そのプロセスでこのプラグインが登録されていません。

```sh
maki auth status 2>&1 | grep -i 'cursor\|warning\|failed to load'
```

- `--no-plugins` を付けないでください。このオプションは `init.lua` を読まないので、プラグインは登録されません。
- パッケージがインストールされ、承認されているか確認します。`~/.local/share/maki/site/pack-approvals.json` に `maki-cursor-provider` があり、`providers` に `cursor` が入っている必要があります。
- トークンの更新に失敗したときは、`maki auth login cursor` をやり直してください。Cursor 側で更新用トークンが無効になっていると、再ログイン以外に復旧できません。
