# yt-dlp 使用自定义 EJS



为了在[开源EJS项目](https://github.com/yt-dlp/ejs) 的失效时有一个备用方案， 所以开发了 [tf-yt-dlp-ejs 项目](https://github.com/tfkxsx/tf-yt-dlp-ejs/releases/tag/0.0.1)



# 使用

tf-yt-dlp-ejs 有两种使用方式, 它们都依赖于 **[ce-yt-dlp](https://github.com/tfkxsx/ce-yt-dlp)**



## 方式一:

> yt-dlp 支持使用自定义github 仓库， 但由于加载顺序(` PYPACKAGE > CACHE > BUILTIN > WEB`) 原因， 导致就算指定了github 仓库也不会提升 WEB 加载优先级。 在 ce-yt-dlp 项目中 custom_ejs 解决了这个问题。



**执行步骤：**

1. 开启 use_custom_plugins 

2. 在optins 中配置 extractor_args 参数：

   ```python
   options = {
     "extractor_args": {
         "youtube": {
             "player_js_version": ['20521@18d29a11'],
             "player_js_variant": ["es6"],
         },
         "youtube-ejs": {
             "dev": ["true"],
             "repo": ["tfkxsx/tfkxsx-yt-dlp-ejs"],
             "script_version": ["0.0.1"],
         },
     }
     "use_custom_plugins": True,
   }
   ```

**参数详解：**

- [可选] player_js_version： youtube固定版本的 player base js 脚本
- [可选] player_js_variant： player base js 脚本来源客户端
- [必填] dev： 跳过脚本hash 值对比
- [必填] repo： 指定自定义的github 仓库
- [必填] script_version： github 仓库中Releases 版本
- [必填] use_custom_plugins:  ce-yt-dlp 项目插件开启总开关



## 方式二：

> ce-yt-dlp 项目集成了完整自定义ejs 功能， 支持开关式启用。只需在optins 中配置参数即可



**在optins 中配置：**

```python
optins = {
  ...
  "use_custom_plugins": True,
  "use_custom_jsc": True,
}
```

**参数详解：**

- use_custom_plugins:	ce-yt-dlp 项目插件开启总开关
- use_custom_jsc：  开启自定义jsc 功能
