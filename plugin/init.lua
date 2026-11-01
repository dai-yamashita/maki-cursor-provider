-- Used when a future maki publishes the checkout as maki.plugin_dir.
-- Maki 0.7 does not, so init.lua loads this package with package.path instead.

local root = maki.plugin_dir
if type(root) ~= "string" or root == "" then
  error("maki-cursor-provider: pass package.path to require('cursor').setup from maki.pack.add")
end
require("cursor").setup(root)
