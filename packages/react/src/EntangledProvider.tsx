import React, { useEffect } from 'react';
import type { QueryClient } from '@tanstack/react-query';
import { startSyncListener, stopSyncListener } from './syncListener';

export interface EntangledProviderProps {
  client: QueryClient;
  children?: React.ReactNode;
}

export function EntangledProvider({ client, children }: EntangledProviderProps) {
  useEffect(() => {
    void startSyncListener(client);
    return () => stopSyncListener();
  }, [client]);

  return <>{children}</>;
}
