---
last_commit_released: d54c27ae6e3b10a733c20546b1f7336b434d7e2d
name: Fable.Reactive
---

# Changelog

All notable changes to this project will be documented in this file.

## 1.0.0-rc.10 - 2026-10-18

### 🐞 Bug Fixes

* Preserve supervised values shaped like child exits (#51) ([3364e37](https://github.com/fable-hub/Fable.Reactive/commit/3364e37ebcb48f21e6fcc4ffebcd609821581e29))
* Correct retry and add Reactive guides (#53) ([d54c27a](https://github.com/fable-hub/Fable.Reactive/commit/d54c27ae6e3b10a733c20546b1f7336b434d7e2d))

<strong><small>[View changes on Github](https://github.com/fable-hub/Fable.Reactive/compare/8314bf23c691a501e6782a98deeb0a461095f7c6..d54c27ae6e3b10a733c20546b1f7336b434d7e2d)</small></strong>

## 1.0.0-rc.9 - 2026-10-17

### 🚀 Features

* Add pipe-friendly subscribeAsync (#42) ([2f317a7](https://github.com/fable-hub/Fable.Reactive/commit/2f317a7f03bf32e7f62c8c13dbeff4a981538f24))
* Add replay subject (#50) ([8314bf2](https://github.com/fable-hub/Fable.Reactive/commit/8314bf23c691a501e6782a98deeb0a461095f7c6))

### 🐞 Bug Fixes

* Restore cross-target subscriptions and actor compatibility (#48) ([cae523a](https://github.com/fable-hub/Fable.Reactive/commit/cae523ad0289ae417a2702ce46bb5edf5131226a))

<strong><small>[View changes on Github](https://github.com/fable-hub/Fable.Reactive/compare/b449860886f32c90c4369842dd275bb0f80fded9..8314bf23c691a501e6782a98deeb0a461095f7c6)</small></strong>

## 1.0.0-rc.8 - 2026-10-03

### 🐞 Bug Fixes

* *(beam)* Keep debounce state actor-owned (#39) ([d408672](https://github.com/fable-hub/Fable.Reactive/commit/d408672d9b636b629a9cacc1a7eeb8d09de5c9ec))

<strong><small>[View changes on Github](https://github.com/fable-hub/Fable.Reactive/compare/2c3fbd69bae145838ea11fbed96b097fdaa31336..b449860886f32c90c4369842dd275bb0f80fded9)</small></strong>

## 1.0.0-rc.7 - 2026-08-01

### 🚀 Features

* *(beam)* Port src to the BEAM target and wire cross-target test runs (#25) ([404772f](https://github.com/fable-hub/Fable.Reactive/commit/404772f4d90aeb1c7d4d542cf7855845d2d0fe81))

### 🐞 Bug Fixes

* Escalate supervisor-monitor failures instead of hanging (#36) ([2c3fbd6](https://github.com/fable-hub/Fable.Reactive/commit/2c3fbd69bae145838ea11fbed96b097fdaa31336))
* *(beam)* Trap exits in the supervisor monitor (#32) ([95ec618](https://github.com/fable-hub/Fable.Reactive/commit/95ec618b0c28b181aeae2bbac6053120526e1089))

<strong><small>[View changes on Github](https://github.com/fable-hub/Fable.Reactive/compare/92de51cf7ea44ad4e87ad802718c558ee9ec6797..2c3fbd69bae145838ea11fbed96b097fdaa31336)</small></strong>

## 1.0.0-rc.6 - 2026-04-06

### 🚀 Features

* Add actor interop operators (#22) ([92de51c](https://github.com/dbrattli/Fable.Reactive/commit/92de51cf7ea44ad4e87ad802718c558ee9ec6797))

<strong><small>[View changes on Github](https://github.com/dbrattli/Fable.Reactive/compare/8278b84003bcb0129afcfb0ff15f3c35ce0dadbe..92de51cf7ea44ad4e87ad802718c558ee9ec6797)</small></strong>

## 1.0.0-rc.5 - 2026-03-14

### 🐞 Bug Fixes

* Strip package name prefix from version in publish workflow ([8278b84](https://github.com/dbrattli/Fable.Reactive/commit/8278b84003bcb0129afcfb0ff15f3c35ce0dadbe))

<strong><small>[View changes on Github](https://github.com/dbrattli/Fable.Reactive/compare/0d90cbe3bed8e14aec100733a1325bae3e1daafc..8278b84003bcb0129afcfb0ff15f3c35ce0dadbe)</small></strong>

## 1.0.0-rc.4 - 2026-03-14

### 🏗️ Breaking changes

* Rename AsyncRx to Reactive (#20) ([0d90cbe](https://github.com/dbrattli/Fable.Reactive/commit/0d90cbe3bed8e14aec100733a1325bae3e1daafc))

### 🐞 Bug Fixes

* Pack AsyncSeq in release and add name to CHANGELOG (#18) ([18aa2bb](https://github.com/dbrattli/Fable.Reactive/commit/18aa2bb522b69941826730527c73d7035c388e6b))

<strong><small>[View changes on Github](https://github.com/dbrattli/Fable.Reactive/compare/3dc12a7a9550cf085e1d464408726d5a1e995973..0d90cbe3bed8e14aec100733a1325bae3e1daafc)</small></strong>

## 1.0.0-rc.3 - 2026-03-14

<strong><small>[View changes on Github](https://github.com/dbrattli/Fable.Reactive/compare/2d029f5a2d1e52acd7921b69adc4c7615bd93c9c..3dc12a7a9550cf085e1d464408726d5a1e995973)</small></strong>

## 1.0.0-rc.2 - 2026-03-14

<strong><small>[View changes on Github](https://github.com/dbrattli/Fable.Reactive/compare/326b27444d1b217c1e99379c0146216389530a51..2d029f5a2d1e52acd7921b69adc4c7615bd93c9c)</small></strong>

## 1.0.0-rc.1

### Features

- Rename from FSharp.Control.AsyncRx to Fable.Reactive
- Rename namespace from FSharp.Control to Fable.Reactive
