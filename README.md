# nucleus-monorepo

## Install Dependencies

```bash
git clone https://github.com/NucleusNews/nucleus-monorepo.git
cd nucleus
npm install
```

## Environment Variables

```bash
cp apps/server/.env.example apps/server/.env
```

```bash
cp apps/fetcher/.env.example apps/fetcher/.env
```

```bash
cp apps/processor/.env.example apps/processor/.env
```

```bash
cp apps/synthesizer/.env.example apps/synthesizer/.env
```

## Running (Development)

```bash
npm run dev:client
npm run dev:server
```

## Building

```bash
npm run build:client
npm run build:server
```
