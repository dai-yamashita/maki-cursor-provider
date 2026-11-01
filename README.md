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

モデルは `cursor/composer-2.5` です。ログインと会話の記録は、パッケージのチェックアウトではなく maki の state ディレクトリにある `maki-cursor-provider/` に置きます。パッケージを更新しても消えません。

プロキシの依存関係は、ログイン時にチェックアウトの `proxy/` へ `npm ci` します。
