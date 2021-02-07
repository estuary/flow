#!/usr/bin/env node
import {bootstraps, transforms} from './catalog/lambdas';
import {main} from './runtime/serve';

main(bootstraps, transforms);
