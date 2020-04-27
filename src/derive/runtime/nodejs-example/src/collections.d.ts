import {StateStore} from "estuary_runtime";

interface Hash111 {
    foo?: string,
    bar?: number
    campaign_id: string,
}

interface Hash222 {
    baz?: number,
    bing: string[],
}

interface Hash333 {
    views: Hash222[],
    clicks: Hash222[],
    inner?: Hash111,
    other?: string,
}

export interface NameAAA extends Hash111 {}
export interface NameBBB extends Hash333 {}
export interface NameCCCForAAA extends Hash222 {}
