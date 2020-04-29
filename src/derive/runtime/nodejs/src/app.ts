#!/usr/bin/env node
import {bootstraps} from './catalog/bootstraps';
import {transforms} from './catalog/transforms';
import {main} from './runtime/serve';

main(bootstraps, transforms);
