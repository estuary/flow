import React, { useEffect, useRef, useState } from 'react';
import ColorModeToggle from '@theme-original/ColorModeToggle';

const MESSAGE_TYPE = 'estuary.colorMode';

export default function ColorModeToggleWrapper(props) {
    const [listenerBound, setListenerBount] = useState(false);
    const { onChange } = props;

    useEffect(() => {
        const handleMessageListener = (event) => {
            if (event.origin === 'http://localhost:3000' || event.origin === 'https://dashboard.estuary.dev') {
                if (event.data?.type === MESSAGE_TYPE) {
                    onChange(event.data.mode);
                }
            }
        };

        if (!listenerBound) {
            window.addEventListener('message', handleMessageListener);
            setListenerBount(true);
        }

        return () => {
            window.removeEventListener('message', handleMessageListener);
        };
    }, []);

    return (
        <>
            <ColorModeToggle {...props} />
        </>
    );
}
